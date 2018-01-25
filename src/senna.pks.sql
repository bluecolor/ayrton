create or replace package  util.senna authid current_user
as

  procedure run(
    i_source_table varchar2,
    i_target_table varchar2, 
    i_parallel number, 
    i_db_link varchar2 default 'BIMSADG',
    i_expar   boolean default false
  );

  procedure load_partition(
    i_source_owner  varchar2,
    i_source_table  varchar2,
    i_target_owner  varchar2,
    i_target_table  varchar2,
    i_db_link       varchar2,
    i_part_num      varchar2,
    i_rowid_from    varchar2,
    i_rowid_to      varchar2,
    i_filter        varchar2 default '1=1',
    i_columns       long default null,
    i_expar         boolean default false
  );

end;