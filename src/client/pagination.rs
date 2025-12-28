use futures::future::join_all;
use std::future::Future;
use tokio::time::{Duration, sleep};

use crate::error::InfraHexError;

use super::types::InfraResult;

/// Configuration for paginated fetching.
#[derive(Debug, Clone)]
pub struct PaginationConfig {
    pub page_size: usize,
    pub batch_size: usize,
    pub batch_delay: Duration,
    pub max_offset: Option<usize>,
}

impl Default for PaginationConfig {
    fn default() -> Self {
        Self {
            page_size: 100,
            batch_size: 100,
            batch_delay: Duration::from_millis(100),
            max_offset: None,
        }
    }
}

impl PaginationConfig {
    /// Creates a new pagination config with OpenDataSoft's 10,000 offset limit.
    pub fn opendatasoft() -> Self {
        Self {
            max_offset: Some(10_000),
            ..Default::default()
        }
    }

    /// Sets the page size.
    pub fn with_page_size(mut self, size: usize) -> Self {
        self.page_size = size;
        self
    }

    /// Sets the batch size.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Sets the delay between batches.
    pub fn with_batch_delay(mut self, delay: Duration) -> Self {
        self.batch_delay = delay;
        self
    }

    /// Sets the maximum offset limit.
    pub fn with_max_offset(mut self, max: usize) -> Self {
        self.max_offset = Some(max);
        self
    }
}

/// Fetches all pages in parallel batches with rate limiting.
///
/// # Arguments
///
/// * `total_count` - Total number of items to fetch
/// * `config` - Pagination configuration
/// * `fetch_page` - Async function that fetches a single page given (offset, limit)
///
/// # Returns
///
/// An `InfraResult` containing all successfully fetched records and any errors.
///
/// # Example
///
/// ```ignore
/// let result = fetch_all_pages(
///     total_count,
///     PaginationConfig::opendatasoft(),
///     |offset, limit| async move {
///         client.fetch_page(offset, limit).await
///     },
/// ).await;
/// ```
pub async fn fetch_all_pages<T, F, Fut>(
    total_count: usize,
    config: PaginationConfig,
    fetch_page: F,
) -> InfraResult<T>
where
    T: Send,
    F: Fn(usize, usize) -> Fut,
    Fut: Future<Output = Result<Vec<T>, InfraHexError>> + Send,
{
    let mut result = InfraResult::new();

    if total_count == 0 {
        return result;
    }

    // Apply max offset limit if configured
    let fetchable = match config.max_offset {
        Some(max) => total_count.min(max),
        None => total_count,
    };

    // Generate all offsets
    let offsets: Vec<usize> = (0..fetchable).step_by(config.page_size).collect();

    // Process in batches
    for chunk in offsets.chunks(config.batch_size) {
        let futures: Vec<_> = chunk
            .iter()
            .map(|&offset| fetch_page(offset, config.page_size))
            .collect();

        let batch_results = join_all(futures).await;

        for page_result in batch_results {
            match page_result {
                Ok(records) => result.records.extend(records),
                Err(e) => result.errors.push(e),
            }
        }

        // Rate limiting delay between batches (skip delay after last batch)
        if !chunk.is_empty() && chunk.last() != offsets.last() {
            sleep(config.batch_delay).await;
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn test_fetch_all_pages_empty() {
        let result: InfraResult<i32> =
            fetch_all_pages(0, PaginationConfig::default(), |_offset, _limit| async {
                Ok(vec![])
            })
            .await;

        assert!(result.records.is_empty());
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_all_pages_single_page() {
        let result = fetch_all_pages(
            50,
            PaginationConfig::default().with_page_size(100),
            |_offset, _limit| async { Ok(vec![1, 2, 3]) },
        )
        .await;

        assert_eq!(result.records, vec![1, 2, 3]);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_all_pages_multiple_pages() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let result = fetch_all_pages(
            250,
            PaginationConfig::default()
                .with_page_size(100)
                .with_batch_delay(Duration::from_millis(1)),
            move |offset, _limit| {
                let cc = call_count_clone.clone();
                async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    Ok(vec![offset as i32])
                }
            },
        )
        .await;

        assert_eq!(call_count.load(Ordering::SeqCst), 3);
        assert_eq!(result.records.len(), 3);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_all_pages_with_max_offset() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let result = fetch_all_pages(
            1000,
            PaginationConfig::default()
                .with_page_size(100)
                .with_max_offset(300)
                .with_batch_delay(Duration::from_millis(1)),
            move |offset, _limit| {
                let cc = call_count_clone.clone();
                async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    Ok(vec![offset as i32])
                }
            },
        )
        .await;

        // Should only fetch up to max_offset (300), so 3 pages
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_all_pages_handles_errors() {
        let result = fetch_all_pages(
            200,
            PaginationConfig::default()
                .with_page_size(100)
                .with_batch_delay(Duration::from_millis(1)),
            |offset, _limit| async move {
                if offset == 0 {
                    Ok(vec![1, 2, 3])
                } else {
                    Err(InfraHexError::Api("Test error".to_string()))
                }
            },
        )
        .await;

        assert_eq!(result.records, vec![1, 2, 3]);
        assert_eq!(result.errors.len(), 1);
    }
}
