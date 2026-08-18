//! Local result pagination arithmetic.

pub(crate) fn page_count(row_count: usize, page_size: usize) -> usize {
    if page_size == 0 {
        return 1;
    }
    row_count.div_ceil(page_size).max(1)
}

pub(crate) fn page_after_page_size_change(
    current_page: usize,
    previous_page_size: usize,
    page_size: usize,
) -> usize {
    if page_size == 0 {
        return 0;
    }
    current_page.saturating_mul(previous_page_size) / page_size
}

#[cfg(test)]
mod tests {
    use super::{page_after_page_size_change, page_count};

    #[test]
    fn pagination_counts_partial_and_empty_pages() {
        assert_eq!(page_count(0, 200), 1);
        assert_eq!(page_count(200, 200), 1);
        assert_eq!(page_count(201, 200), 2);
        assert_eq!(page_after_page_size_change(3, 200, 500), 1);
    }
}
