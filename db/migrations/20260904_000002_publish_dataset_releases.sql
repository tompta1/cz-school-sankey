begin;

create index if not exists dataset_release_published_lookup_idx
  on meta.dataset_release (domain_code, dataset_code, reporting_year, published_at desc)
  where status = 'published';

update meta.dataset_release
set status = 'published',
    published_at = coalesce(published_at, fetched_at)
where status = 'staged'
  and coalesce(row_count, 0) > 0;

commit;
