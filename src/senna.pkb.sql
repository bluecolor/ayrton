CREATE OR REPLACE package body util.SENNA
AS
  gv_pkg varchar2(30) := 'SENNA'; 	
  gv_sql long;
  gv_sql_errm     varchar2(4000);                                -- SQL Error Message
  gv_sql_errc     number;                                        -- SQL Error Code
  gv_proc varchar2(30);
  gv_param_owner varchar2(30) := 'UTIL';
  gv_param_table varchar2(30) := 'SENNA_PARAMS';
  gv_owner varchar2(30) := 'SG';
  gv_dblk  varchar2(30) := 'DWHPROD';
  gv_max_parallel number := 16;

  type rowid_cursor_type is ref cursor;
  type rowid_array_type  is varray(2) of varchar2(255);
  type rowid_table_type  is table of rowid_array_type;


  function get_range_query(
    i_table    varchar2, 
    i_owner    varchar2 default gv_owner,
    i_dblk     varchar2 default gv_dblk,  
    i_parallel number   default gv_max_parallel
  ) return long;

  function  get_param(i_name varchar2) return varchar2;
  procedure set_param(i_name varchar2, i_value varchar2);
  procedure reset_param(i_name varchar2);
  function  get_columns(i_owner varchar2, i_table varchar2) return long;
  function  get_columns(i_table varchar2) return long;

  procedure load_partition_expar(
    i_source_owner  varchar2,
    i_source_table  varchar2,
    i_target_owner  varchar2,
    i_target_table  varchar2,
    i_db_link       varchar2,
    i_part_num      varchar2,
    i_rowid_from    varchar2,
    i_rowid_to      varchar2,
    i_filter        varchar2 default '1=1',
    i_columns       long default null
  );

  procedure load_partition_insert(
    i_source_owner  varchar2,
    i_source_table  varchar2,
    i_target_owner  varchar2,
    i_target_table  varchar2,
    i_db_link       varchar2,
    i_part_num      varchar2,
    i_rowid_from    varchar2,
    i_rowid_to      varchar2,
    i_filter        varchar2 default '1=1',
    i_columns       long default null
  );  
  
  function get_param(i_name varchar2) return varchar2
  is
    v_val varchar2(4000);
  begin
    execute immediate '
      select value from '||gv_param_owner||'.' || gv_param_table||'
      where upper(name) = '''||upper(i_name)||'''
    ' into v_val;

    return v_val;
    exception when others then return null;
  end;

  procedure set_param(i_name varchar2, i_value varchar2)
  is
    pragma autonomous_transaction;
  begin
    execute immediate '
      merge into '||gv_param_owner||'.'||gv_param_table||' d
      using (
        select '''||upper(i_name)||''' name, '''||i_value||''' value from dual 
      ) s
      on (upper(d.name) = s.name )
      when matched then update
        set d.value = s.value
      when not matched then insert
        (name,value) values(s.name, s.value)
    ';
    commit;
    exception when others then rollback;
  end;

  procedure reset_param(i_name varchar2) is 
  begin
    gv_sql := '
      DELETE FROM '||gv_param_owner||'.'||gv_param_table||'
      WHERE upper(name) = '''||upper(i_name)||'''
    ';
    execute immediate gv_sql;
    commit;
  
  exception
    when OTHERS then
      gv_sql_errc := SQLCODE;
      gv_sql_errm := SQLERRM;
      pl.logger.error(gv_sql_errm, gv_sql);
      rollback;
      raise_application_error(gv_sql_errc, gv_sql_errm);
  end;

  function get_columns(i_owner varchar2, i_table varchar2) return long is
    v_columns long;
  begin
    select listagg(column_name,',') within group(order by column_name) into v_columns
    from all_tab_cols 
    where owner = upper(i_owner) and table_name = upper(i_table) and column_name not in ('ROW_ID', 'PART_NUM');
    return v_columns;
  end;

  function get_columns(i_table varchar2) return long is 
    v_table dbms_sql.varchar2_table := pl.split(i_table, '.');
  begin
    return get_columns(v_table(1), v_table(2));
  end;

  function get_range_query(
    i_table    varchar2, 
    i_owner    varchar2 default gv_owner,
    i_dblk     varchar2 default gv_dblk,  
    i_parallel number   default gv_max_parallel
  ) return long
  is
    v_sql long;
  begin
    
    v_sql := '
      SELECT 
        --------------------------------------------------------------------------
        dbms_rowid.rowid_create (1,
                                data_object_id,
                                lo_fno,
                                lo_block,
                                0) ROWID_FROM,
        --------------------------------------------------------------------------
        dbms_rowid.rowid_create (1,
                                data_object_id,
                                hi_fno,
                                hi_block,
                                10000) ROWID_TO
        --------------------------------------------------------------------------
      FROM (WITH c1 AS
              (SELECT   *
                    FROM dba_extents@'||i_dblk||' 
                  WHERE segment_name = UPPER ('''||i_table||''')
                    AND owner = UPPER ('''||i_owner||''')
                ORDER BY block_id)
          SELECT DISTINCT grp,
              FIRST_VALUE (relative_fno) OVER (PARTITION BY grp ORDER BY relative_fno,
              block_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lo_fno,
              FIRST_VALUE (block_id) OVER (PARTITION BY grp ORDER BY relative_fno,
              block_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lo_block,
              LAST_VALUE (relative_fno) OVER (PARTITION BY grp ORDER BY relative_fno,
              block_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS hi_fno,
              LAST_VALUE (block_id + blocks - 1) OVER (PARTITION BY grp ORDER BY relative_fno,
              block_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS hi_block,
              SUM (blocks) OVER (PARTITION BY grp) AS sum_blocks
          FROM (SELECT   relative_fno, block_id, blocks,
                          TRUNC(  (  SUM (blocks) OVER (ORDER BY relative_fno,block_id)- 0.01)
                                / (SUM (blocks) OVER () / '||i_parallel||') ) grp
                    FROM c1
                    WHERE segment_name = UPPER ('''||i_table||''')
                    AND owner = UPPER ('''||i_owner||''')
                ORDER BY block_id)),
        (SELECT data_object_id
            FROM all_objects@'||i_dblk||'  
          WHERE object_name = UPPER ('''||i_table||''') AND owner = UPPER ('''||i_owner||'''))
    ';
    
    return v_sql;

  end;
  
  procedure run(
    i_source_table varchar2,
    i_target_table varchar2, 
    i_parallel     number, 
    i_db_link      varchar2 default 'BIMSADG',
    i_expar        boolean default false,
    i_filter       varchar2 default null
  ) is
    ----------------------------------------------------------------------------
    v_child_process_error exception;
    pragma exception_init(v_child_process_error, -20001 );
    v_rowid_query   long;
    v_rowid_cursor  rowid_cursor_type;
    v_rowid_from    varchar2(255);
    v_rowid_to      varchar2(255);
    v_thread_no     number := 1;
    v_job_action    varchar2(4000);
    v_sleep_sec     number := 5;
    v_has_error     varchar2(10) := '0';
	  v_parallel      number := 0; -- actual parallelism
    v_completed_key varchar2(200) := i_source_table||'@'||i_db_link||':COMPLETED';
    v_error_key     varchar2(200) := i_source_table||'@'||i_db_link||':ERROR';
    v_columns       long := get_columns(i_target_table);
    v_source        dbms_sql.varchar2_table := pl.split(i_source_table, '.');
	  v_target        dbms_sql.varchar2_table := pl.split(i_target_table, '.');
	----------------------------------------------------------------------------
  begin
    pl.p('Starting run proc for ' || i_source_table||'@'||i_db_link);

	  gv_proc := 'RUN';
    pl.logger := utl.logtype.init(gv_pkg ||'.'||gv_proc);

    -- clean params table for the run
    reset_param(v_completed_key);
    reset_param(v_error_key);

    -- reset completed child count to zero
    set_param(v_completed_key,0);

    -- reset child process error
    set_param(v_error_key,0);

    pl.p('Get rowid range query ....');
    -- get rowid query for given parallel limit
    v_rowid_query := get_range_query(
      i_owner    => v_source(1),
      i_table    => v_source(2),
      i_parallel => i_parallel
    );

    pl.p('Range Quuery:');
    pl.p(v_rowid_query);

    pl.p('Truncating target table '||i_target_table);
    pl.truncate_table(i_target_table);
    
    pl.p('Starting child processes ...');
    open v_rowid_cursor for v_rowid_query;
    loop
      -- create an async job for each rowid-range
      fetch v_rowid_cursor into v_rowid_from, v_rowid_to;
      exit when v_rowid_cursor%notfound;

        v_parallel := v_parallel + 1;

        -- script to execute in async-job
        v_job_action := '
          begin 
            util.senna.load_partition(
              i_source_owner => '''||v_source(1)||''',
              i_source_table => '''||v_source(2)||''',
              i_target_owner => '''||v_target(1)||''',
              i_target_table => '''||v_target(2)||''',
              i_db_link    => '''||i_db_link   ||''',
              i_part_num   => '  ||v_thread_no ||'  ,
              i_rowid_from => '''||v_rowid_from||''',     
              i_rowid_to   => '''||v_rowid_to  ||''',
              i_columns    => '''||v_columns   ||''',
              i_filter     => '''||i_filter    ||''',
              i_expar      => '||case i_expar when true then 'true' else 'false' end|| '); 
          end;
        ';

        pl.p('Scheduling :'); pl.p(v_job_action);
        
        -- invoke the async-job
        dbms_scheduler.create_job (  
          job_name      =>  v_source(2)||'$'||v_thread_no,  
          job_type      =>  'PLSQL_BLOCK',  
          job_action    =>  v_job_action,  
          start_date    =>  sysdate - 1/24,  
          enabled       =>  true,  
          auto_drop     =>  true,  
          comments      =>  i_source_table||'@'||i_db_link||'#'||v_thread_no);
        
        pl.logger.info('Started '|| i_source_table||'@'||i_db_link||'#'||v_thread_no);
        v_thread_no := v_thread_no + 1;
      end loop;  


    -- join: wait for processes to complete
    loop
      pl.p('Check completion ...');
      pl.sleep(v_sleep_sec * 1000);
      v_has_error := get_param(v_error_key);
      if v_has_error is not null and v_has_error != '0'  then
        raise_application_error( -20001, v_error_key || ': One of the childs has error. Kill others and restart!' );
      end if;
      exit when v_parallel <= to_number(trim(get_param(v_completed_key)));
    end loop;

  exception
    when others then
      gv_sql_errc := SQLCODE;
      gv_sql_errm := SQLERRM;
      pl.logger.error( gv_sql_errc||':'||gv_sql_errm, v_job_action);
      rollback;
      raise_application_error(gv_sql_errc, gv_sql_errm);
  end;

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
  ) is 
  begin
    if i_expar = true then
      load_partition_expar(
        i_source_owner,
        i_source_table,
        i_target_owner,
        i_target_table,
        i_db_link,
        i_part_num,
        i_rowid_from,
        i_rowid_to,
        i_filter,
        i_columns
      );
    else
      load_partition_insert(
        i_source_owner,
        i_source_table,
        i_target_owner,
        i_target_table,
        i_db_link,
        i_part_num,
        i_rowid_from,
        i_rowid_to,
        i_filter,
        i_columns
      );
    end if;

  end;


  procedure load_partition_expar(
    i_source_owner  varchar2,
    i_source_table  varchar2,
    i_target_owner  varchar2,
    i_target_table  varchar2,
    i_db_link       varchar2,
    i_part_num      varchar2,
    i_rowid_from    varchar2,
    i_rowid_to      varchar2,
    i_filter        varchar2 default '1=1',
    i_columns       long default null
  ) is
    ----------------------------------------------------------------------------
    v_completed_key varchar2(200) := i_source_owner||'.'||i_source_table||'@'||i_db_link||':COMPLETED';
    v_error_key     varchar2(200) := i_source_owner||'.'||i_source_table||'@'||i_db_link||':ERROR';
    ----------------------------------------------------------------------------
    v_partition     varchar2(30) := 'P'||i_part_num;
    v_filter        long;
    v_columns       long := i_columns;
    v_table_name    varchar2(32) := substr(i_target_table,1,28)||'$'||v_partition;
    ----------------------------------------------------------------------------
    procedure done
    is
      v_completed_process number := 0;
    begin
      -- lock orther jobs
      execute immediate '
        merge into '||gv_param_owner||'.'||gv_param_table||' d
        using (
          select '''||upper(v_completed_key)||''' name from dual 
        ) s
        on (upper(d.name) = s.name )
        when matched then update
          set d.value = to_number(d.value) + 1
        when not matched then insert
          (name,value) values(s.name, 1)
      ';
      commit;
    end;
    ----------------------------------------------------------------------------
    procedure error
    is
      v_error_process number := 0;
    begin
      v_error_process := to_number(nvl(get_param(v_error_key),'0'));
      v_error_process := v_error_process + 1;
      set_param(v_error_key, v_error_process);
    end;
    ----------------------------------------------------------------------------
  begin
    gv_proc := 'LOAD_PARTITION_EXPAR';
    pl.logger := utl.logtype.init(gv_pkg ||'.'||gv_proc);

    if i_filter is null then 
      v_filter := ' 1=1 ';
    else
      v_filter := i_filter;
    end if;

    v_filter := v_filter|| ' AND
      rowid between '''|| i_rowid_from||''' and '''|| i_rowid_to||'''
    ';

    if i_columns is null then 
      v_columns := get_columns(i_target_owner, i_target_table); 
    end if;

    pl.drop_table(i_target_owner, v_table_name);

    gv_sql := '
      CREATE TABLE '|| i_target_owner ||'.'|| v_table_name ||'
      PARALLEL NOLOGGING COMPRESS
      AS
      SELECT --+ parallel(t, 16)
        '||i_part_num||' PART_NUM,
        '||v_columns ||'
      FROM
        '||i_source_owner||'.'||i_source_table||'@'||i_db_link||' t
      WHERE
        '||v_filter||'
    ';
    pl.enable_parallel_dml;
    execute immediate gv_sql;    
    pl.logger.success(SQL%ROWCOUNT,gv_sql); 
    
    pl.exchange_partition(
      i_owner     => i_target_owner, 
      i_table_1   => i_target_table, 
      i_part_name => v_partition,
      i_table_2   => i_target_owner ||'.'|| v_table_name 
    );
    
    done;

  exception
    WHEN OTHERS THEN
      gv_sql_errc := SQLCODE;
      gv_sql_errm := SQLERRM;
      pl.logger.error(gv_sql_errm, gv_sql);
      rollback;
      error;
      raise_application_error(gv_sql_errc, gv_sql_errm);
  end;



  procedure load_partition_insert(
    i_source_owner  varchar2,
    i_source_table  varchar2,
    i_target_owner  varchar2,
    i_target_table  varchar2,
    i_db_link       varchar2,
    i_part_num      varchar2,
    i_rowid_from    varchar2,
    i_rowid_to      varchar2,
    i_filter        varchar2 default '1=1',
    i_columns       long default null
  ) is
    ----------------------------------------------------------------------------
    v_completed_key varchar2(200) := i_source_owner||'.'||i_source_table||'@'||i_db_link||':COMPLETED';
    v_error_key     varchar2(200) := i_source_owner||'.'||i_source_table||'@'||i_db_link||':ERROR';
    ----------------------------------------------------------------------------
    v_partition     varchar2(30) := 'P'||i_part_num;
    v_filter        long;
    v_columns       long := i_columns;
    ----------------------------------------------------------------------------
    procedure done
    is
      v_completed_process number := 0;
    begin
      -- lock orther jobs
      execute immediate '
        merge into '||gv_param_owner||'.'||gv_param_table||' d
        using (
          select '''||upper(v_completed_key)||''' name from dual 
        ) s
        on (upper(d.name) = s.name )
        when matched then update
          set d.value = to_number(d.value) + 1
        when not matched then insert
          (name,value) values(s.name, 1)
      ';
      commit;

      -- !!!Deprecated!!!
      -- This may cause starvation
      -- v_completed_process := to_number(nvl(get_param(v_completed_key),'0'));
      -- v_completed_process := v_completed_process + 1;
      -- set_param(v_completed_key,v_completed_process);
    end;
    ----------------------------------------------------------------------------
    procedure error
    is
      v_error_process number := 0;
    begin
      v_error_process := to_number(nvl(get_param(v_error_key),'0'));
      v_error_process := v_error_process + 1;
      set_param(v_error_key, v_error_process);
    end;
    ----------------------------------------------------------------------------
  begin
    gv_proc := 'LOAD_PARTITION_INSERT';
    pl.logger := utl.logtype.init(gv_pkg ||'.'||gv_proc);

    if i_filter is null then 
      v_filter := ' 1=1 ';
    else
      v_filter := i_filter;
    end if;

    v_filter := v_filter|| ' AND
      rowid between :rowid_from and :rowid_to
    ';

    if i_columns is null then 
      v_columns := get_columns(i_target_owner, i_target_table); 
    end if;

    gv_sql := '
      INSERT /*+ append nologging parallel */ INTO '|| i_target_owner ||'.'|| i_target_table ||'
      PARTITION('||v_partition||')
      ( 
        PART_NUM,
        '||v_columns ||'   
      )
      SELECT --+ parallel(t, 16)
        '||i_part_num||' PART_NUM,
        '||v_columns ||'
      FROM
        '||i_source_owner||'.'||i_source_table||'@'||i_db_link||' t
      WHERE
        '||v_filter||'
    ';
    
    pl.enable_parallel_dml;
    execute immediate gv_sql using i_rowid_from, i_rowid_to;    
    pl.logger.success(SQL%ROWCOUNT,gv_sql); 
    commit;   
    done;

  exception
    WHEN OTHERS THEN
      gv_sql_errc := SQLCODE;
      gv_sql_errm := SQLERRM;
      pl.logger.error(gv_sql_errm, gv_sql);
      rollback;
      error;
      raise_application_error(gv_sql_errc, gv_sql_errm);
  end;

end;