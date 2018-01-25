CREATE OR REPLACE PACKAGE BODY DWH.PKG_INV_INVOICE
IS

--------------------------------------------------------------------------------
--  Created By              : Ceyhun Kerti
--  Creation Date           : 2017.03.15
--  Last Modification Date  : 
--  Package Name            : PKG_INV_INVOICE
--  Package Version         : 0.0.8
--  Package Description     : 
--
--  Modification History]
--------------------------------------------------------------------------------
--


  -----------------------------------
  -- Initialize Log Variables      --
  -----------------------------------
  gv_job_module   constant varchar2(50)  := 'INV';               -- Job Module Name
  gv_pck          constant varchar2(50)  := 'PKG_INV_INVOICE';   -- PLSQL Package Name
  gv_job_owner    constant varchar2(50)  := 'DWH';               -- Owner of the Job
  gv_proc         varchar2(100);                                 -- Procedure Name
  gv_delim        varchar2(10)  := ' : ';                        -- Delimiter Used In Logging
    
  gv_sql_errm     varchar2(4000);                                -- SQL Error Message
  gv_sql_errc     number;                                        -- SQL Error Code
  gv_dyn_task     long := '';
  gv_high_date    constant date := to_date('2999.01.01','yyyy.mm.dd');
  gv_max_rowid_parallel number := 16;

  -- schemas
  gv_ods_mbs_dicle_owner  constant varchar2(30)  := 'ODS_MBS_DICLE';
  gv_mbs_dicle_owner  constant varchar2(30)  := 'DICLE';
  gv_dwh_owner        constant varchar2(30)  := 'DWH';
  gv_edw_owner        constant varchar2(30)  := 'DWH_EDW';
  gv_stg_owner        constant varchar2(30)  := 'DWH';
  gv_inv_param_table  constant varchar2(30)  := 'INV_PARAMS';
  gv_inv_param_owner  constant varchar2(30)  := 'DWH';

  gv_ded_wondow_size  constant number  := 366*10;
  gv_dc_wondow_size   constant number  := 366*10;

  --status codes
  gv_ext_status_idle    constant varchar2(50) := 'IDLE';
  gv_ext_status_running constant varchar2(50) := 'RUNNING';
  gv_ext_status_success constant varchar2(50) := 'SUCCESS';
  gv_ext_status_error   constant varchar2(50) := 'ERROR';

  --db links
  gv_dblk_mbs  varchar2(20) := 'DBLINKORCLMBSDG';

  type rowid_cursor_type is ref cursor;
  type rowid_array_type  is varray(2) of varchar2(255);
  type rowid_table_type  is table of rowid_array_type;

  ------------------------------------------------------------------------------
  -- private method declerations
  function  fun_get_inv_param(fiv_name varchar2) return varchar2;

  procedure prc_set_inv_param(piv_name varchar2, piv_value varchar2);

  function fun_is_inv_active return boolean;

  function fun_get_rowid_range_query(
    fiv_owner varchar2 default gv_mbs_dicle_owner,
    fiv_table varchar2 default 'FATURA',
    fiv_db_link  varchar2 default gv_dblk_mbs,
    fin_parallel number default gv_max_rowid_parallel
  ) return long;

  procedure sleep(pin_millis IN NUMBER) 
  as language java name 'java.lang.Thread.sleep(long)';  
  ------------------------------------------------------------------------------

  ------------------------------------------------------------------------------
  --  Created By     : Ceyhun Kerti
  --  Creation Date  : 2017.04.14
  --  Function Name  : fun_get_inv_param
  --  Description    :  
  --
  --  [Modification History]
  --  --------------------------------------------------------------------------
  function fun_get_inv_param(fiv_name varchar2) return varchar2
  is
    v_param_val varchar2(4000);
  begin

    execute immediate '
      select value from '||gv_inv_param_owner||'.'||gv_inv_param_table||'
      where upper(name) = '''||upper(fiv_name)||'''
    ' into v_param_val;

    return v_param_val;

    exception when others then return null;
  end;

  ------------------------------------------------------------------------------
  --  Created By     : Ceyhun Kerti
  --  Creation Date  : 2017.04.14
  --  Procedure Name : prc_set_inv_param
  --  Description    :  
  --
  --  [Modification History]
  --  --------------------------------------------------------------------------
  procedure prc_set_inv_param(piv_name varchar2, piv_value varchar2)
  is
    pragma autonomous_transaction;
  begin
    execute immediate '
      merge into '||gv_inv_param_owner||'.'||gv_inv_param_table||' d
      using (
        select '''||upper(piv_name)||''' name, '''||piv_value||''' value from dual 
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

  ------------------------------------------------------------------------------
  --  Created By     : Ceyhun Kerti
  --  Creation Date  : 2017.04.14
  --  Function Name  : fun_get_rowid_range_query
  --  Description    :  
  --
  --  [Modification History]
  --  --------------------------------------------------------------------------
  function fun_get_rowid_range_query(
    fiv_owner varchar2 default gv_mbs_dicle_owner,
    fiv_table varchar2 default 'FATURA',
    fiv_db_link  varchar2 default gv_dblk_mbs,  
    fin_parallel number default gv_max_rowid_parallel
  ) return long
  is
    v_rowid_sql long;
  begin
    
    v_rowid_sql := '
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
                    FROM dba_extents@'||fiv_db_link||' 
                  WHERE segment_name = UPPER ('''||fiv_table||''')
                    AND owner = UPPER ('''||fiv_owner||''')
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
                                / (SUM (blocks) OVER () / '||fin_parallel||') ) grp
                    FROM c1
                    WHERE segment_name = UPPER ('''||fiv_table||''')
                    AND owner = UPPER ('''||fiv_owner||''')
                ORDER BY block_id)),
        (SELECT data_object_id
            FROM all_objects@'||fiv_db_link||'  
          WHERE object_name = UPPER ('''||fiv_table||''') AND owner = UPPER ('''||fiv_owner||'''))
    ';
    
    return v_rowid_sql;

  end;

  ------------------------------------------------------------------------------
  --  Created By     : Ceyhun Kerti
  --  Creation Date  : 2017.03.15
  --  Function Name  : fun_is_inv_active
  --  Description    :  
  --
  --  [Modification History]
  --  --------------------------------------------------------------------------
  function  fun_is_inv_active return boolean
  is
    v_active      varchar2(4000) := trim(fun_get_inv_param('FATURA_LIVE_ACTIVE'));
    v_start_time  varchar2(4) := trim(fun_get_inv_param('FATURA_LIVE_START_TIME'));
    v_end_time    varchar2(4) := trim(fun_get_inv_param('FATURA_LIVE_END_TIME'));
  begin

    if nvl(v_active,'0') = '0' then
      return false;
    end if;
    
    if v_start_time is null or v_end_time is null then 
      return true; 
    end if; 

    if sysdate between 
        to_date(to_char(sysdate,'yyyymmdd')||v_start_time,'yyyymmddhh24mi') and
        to_date(to_char(sysdate,'yyyymmdd')||v_end_time,'yyyymmddhh24mi') 
    then 
      return true;
    end if;

    return false;
    
  end;


  ------------------------------------------------------------------------------
  --  Created By     : Ceyhun Kerti
  --  Creation Date  : 2017.03.15
  --  Procedure Name : PRC_TINV0000_FATURA
  --  Description    :  
  --    pin_inv_type(1|2|3) => 1:Tahakkuk, 2:Tahsilat, 3:Alacak
  --  [Modification History]
  --  --------------------------------------------------------------------------
  PROCEDURE PRC_TINV0000_FATURA(pin_inv_type varchar2 default 1)
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30)  := 'TINV0000_FATURA_'||to_char(pin_inv_type);  
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30)  := 'FATURA';
    ----------------------------------------------------------------------------
    v_filter  long;  
    v_db_link varchar2(4000) := fun_get_inv_param('FATURA_EXT_DBLK');
    v_select_hint varchar2(4000) := fun_get_inv_param('FATURA_LIVE_SELECT_HINT');
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_TINV0000_FATURA';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    -- check if the procudere can be executed
    if fun_is_inv_active = false then
      plib.o_log.log( null, null, 'proc is not active ', null, null,null);
      goto END_PROC;
    end if;  

    if pin_inv_type = 1 then    -- Tahakkuk
      v_filter := '
        (
          TAHAKKUK_TARIHI = to_number(to_char(sysdate,''yyyymmdd'')) 
        )
      ';
    elsif pin_inv_type = 2 then -- Tahsilat
      v_filter := '
        (
          KAPATMA_TARIHI  = to_number(to_char(sysdate,''yyyymmdd''))
        )';
    elsif pin_inv_type = 3 then -- Alacak
      v_filter := '
        (
          KAPATMA_TARIHI is null or
          KAPATMA_TARIHI = 0
        )
      ';
    else
      plib.o_log.log( null, null, 'unknown <<inv_type>> argument '||pin_inv_type, null,null);
      goto END_PROC;
    end if; 

    
    plib.drop_table(gv_stg_owner, v_table_name);

    gv_dyn_task := '
      CREATE TABLE '|| gv_stg_owner ||'.'||v_table_name||' 
      parallel nologging compress
      AS
      SELECT 
        /*+
          '||v_select_hint||'
        */
        t1.FATURA_SERI,              
        t1.FATURA_NO,                
        t1.FATURA_TIPI,              
        t1.TESISAT_NO,               
        t1.SOZLESME_NO,              
        t1.MUTA_YIL,                 
        t1.MUTA_NO,                  
        t1.ILK_OKUMA_TARIHI,         
        t1.OKUMA_TARIHI,             
        t1.TAHAKKUK_TARIHI,          
        t1.TANZIM_TARIHI,            
        t1.SON_ODEME_TARIHI,         
        t1.UNVAN,                    
        t1.TARIFE_KODU,
        t1.AKTIF_KWH,                
        t1.AKTIF_MIKTAR,             
        t1.REAKTIF_MIKTAR,           
        t1.SATICI_KWH,               
        t1.DAGITIM_BEDELI,           
        t1.TOPLAM_TUKETIM,           
        t1.EE_FONU,                  
        t1.TRT_PAYI,                 
        t1.BELEDIYE_VERGISI,         
        t1.TOPLAM_KDV,               
        t1.DEVIR_YUVARLAMA,          
        t1.FATURA_TUTAR,             
        t1.TOPLAM_TUTAR,             
        t1.GECIKME_ZAMMI_TUTAR,      
        t1.GECIKME_ZAMMI_KDV,        
        t1.AKIBET_DUR,               
        t1.FATURA_KODU,              
        t1.FATURA_NEDENI,            
        t1.IPTAL_NEDENI,             
        t1.BOLGE_KODU,               
        t1.F_BOLGE_KODU,             
        t1.OKUYUCU_KODU,             
        t1.OKUMA_SAATI,              
        t1.BAGLANTI_DUR,             
        t1.OG_DUR,                   
        t1.KAPATMA_TARIHI,           
        t1.KAPATMA_VEZNE,            
        t1.KAPATMA_VEZNEDAR,         
        t1.KAPATMA_SAATI,            
        t1.KAPATMA_SUBE,             
        t1.KAPATMA_SEKLI,            
        t1.KAPATMA_SEKLI_BANKA_KODU, 
        t1.KODLAMA_TARIHI,           
        t1.SIRKET_KODU,              
        t1.TEDARIK_DUR
      FROM
        '||gv_mbs_dicle_owner ||'.'||v_src_table_01||'@'||nvl(v_db_link, gv_dblk_mbs)||' t1
      WHERE
        '||v_filter||'
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);    

    commit;

    <<END_PROC>>null;

    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;

  ------------------------------------------------------------------------------
  --  Created By     : Ceyhun Kerti
  --  Creation Date  : 2017.03.15
  --  Procedure Name : PRC_INV_MERGE_INVOICE
  --  Description    :  
  --  
  --  [Modification History]
  --  --------------------------------------------------------------------------
  PROCEDURE PRC_INV_INVOICE_LIVE( 
    pin_inv_type  varchar2 default 1
  )
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30)   := 'INV_INVOICE_LIVE';
    v_partition     varchar2(30);
    v_inv_type      varchar2(50);
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30)   := 'TINV0000_FATURA_'||pin_inv_type;
    v_src_table_02  varchar2(30)   := 'TEMP_DEVIR_TEDAS';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_INV_INVOICE_LIVE';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
    
    -- check if the procudere can be executed
    if fun_is_inv_active = false then
      plib.o_log.log( null, null, 'proc is not active ', null, null,null);
      goto END_PROC;
    end if;  

    v_partition :=
      case pin_inv_type 
        when 1 then 'partition(p1)' 
        when 2 then 'partition(p2)' 
        when 3 then 'partition(p3)'
        else        'partition(p0)' 
      end;
    
    v_inv_type :=
      case pin_inv_type 
        when 1 then 'TAHAKKUK' 
        when 2 then 'TAHSILAT' 
        when 3 then 'ALACAK'
        else        'X' 
      end;
    

    ----------------------------------------------------------------------------
    gv_dyn_task := '
      DELETE FROM '||gv_dwh_owner||'.'||v_table_name||'
      WHERE INV_TYPE = '''||v_inv_type||'''
    ';
    execute immediate gv_dyn_task;
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    ----------------------------------------------------------------------------

    gv_dyn_task := '
      INSERT /*+ append nologging */ INTO '|| gv_dwh_owner ||'.'||v_table_name||' '|| v_partition||'
      (
        INV_TYPE,
        INVOICE_SERIAL,
        INVOICE_NUMBER,
        INSTALLATION_ID,
        INVOICE_TYPE,  
        INVOICE_STATUS,
        SUPPLY_SITUATION,
        INVOICE_CAUSE,
        COMPANY_CODE,
        CANCEL_CAUSE,
        NAME,
        CONTRACT_NUMBER,
        DISTRICT_CODE,
        I_DISTRICT_CODE,
        TARIFF_CODE,
        READER_CODE,
        READER_TIME,
        LEAKAGE_YEAR,
        LEAKAGE_NUMBER,
        INVOICE_DATE,
        FIRST_READ_DATE,
        LAST_READ_DATE,
        READ_DAY,
        LAST_PAYMENT_DATE,
        KWH_ACTIVE,
        KWH_ACTIVE_AMOUNT,
        KWH_SELLER,
        KWH_ADDITION,
        KWH_TOTAL,
        KWH_REAKTIF,
        TL_TOTAL_TAX,
        TL_DISTRIBUTION_COST,
        TL_EE_FUND,
        TL_TRT_SHARE,
        TL_COUNCIL_TAX,
        TL_TURNOVER_ROUNDING,
        TL_TOTAL_CONSUMPTION,
        TL_INVOICE_TOTAL,
        TL_GENERAL_TOTAL,
        TL_DEFAULT_INTEREST,
        TL_DEFAULT_INTEREST_TAX,
        CLOSING_DATE,
        CLOSING_TIME,
        CLOSING_TELLERS,
        CLOSING_CASH_DESK,
        CLOSING_BRANCH,
        CLOSING_SHAPE,
        CLOSING_BANK_CODE_SHAPE,
        STATUS_TYPE_ID,
        IS_GOVERNMENT,
        CONNECTION_TYPE,
        BANK_COLLECTION_DATE,
        VOLTAGE_TYPE,
        ARRANGEMENT_DATE
      )
      SELECT --+ parallel(t1, 16)
        '''||v_inv_type||'''  INV_TYPE,
        t1.FATURA_SERI      INVOICE_SERIAL, 
        t1.FATURA_NO        INVOICE_NUMBER, 
        t1.TESISAT_NO       INSTALLATION_ID,
        t1.FATURA_TIPI      INVOICE_TYPE,   
        t1.FATURA_KODU      INVOICE_STATUS, 
        t1.TEDARIK_DUR      SUPPLY_SITUATION,
        t1.FATURA_NEDENI    INVOICE_CAUSE,
        t1.SIRKET_KODU      COMPANY_CODE,        
        t1.IPTAL_NEDENI     CANCEL_CAUSE,
        t1.UNVAN            NAME,
        t1.SOZLESME_NO      CONTRACT_NUMBER,
        t1.BOLGE_KODU       DISTRICT_CODE,
        t1.F_BOLGE_KODU     I_DISTRICT_CODE,
        t1.TARIFE_KODU      TARIFF_CODE,
        t1.OKUYUCU_KODU     READER_CODE,
        t1.OKUMA_SAATI      READER_TIME,
        t1.MUTA_YIL         LEAKAGE_YEAR,
        t1.MUTA_NO          LEAKAGE_NUMBER,
        t1.TAHAKKUK_TARIHI  INVOICE_DATE,
        t1.ILK_OKUMA_TARIHI FIRST_READ_DATE,
        t1.OKUMA_TARIHI     LAST_READ_DATE,
        ------------------------------------------------------------------------
        case 
          when t1.OKUMA_TARIHI is not null and t1.ILK_OKUMA_TARIHI is not null
          then t1.OKUMA_TARIHI - t1.ILK_OKUMA_TARIHI
          else 0
        end READ_DAY,
        ------------------------------------------------------------------------
        t1.SON_ODEME_TARIHI LAST_PAYMENT_DATE,
        t1.AKTIF_KWH        KWH_ACTIVE,
        t1.AKTIF_MIKTAR     KWH_ACTIVE_AMOUNT,
        t1.SATICI_KWH       KWH_SELLER,
        0                   KWH_ADDITION,
        ------------------------------------------------------------------------
        nvl2(t1.SATICI_KWH,AKTIF_KWH,t1.SATICI_KWH) KWH_TOTAL,
        ------------------------------------------------------------------------
        t1.REAKTIF_MIKTAR       KWH_REAKTIF,
        t1.TOPLAM_KDV           TL_TOTAL_TAX,
        t1.DAGITIM_BEDELI       TL_DISTRIBUTION_COST,
        t1.EE_FONU              TL_EE_FUND,
        t1.TRT_PAYI             TL_TRT_SHARE,
        t1.BELEDIYE_VERGISI     TL_COUNCIL_TAX,
        t1.DEVIR_YUVARLAMA      TL_TURNOVER_ROUNDING,
        t1.TOPLAM_TUKETIM       TL_TOTAL_CONSUMPTION,
        t1.FATURA_TUTAR         TL_INVOICE_TOTAL,
        t1.TOPLAM_TUTAR         TL_GENERAL_TOTAL,
        t1.GECIKME_ZAMMI_TUTAR  TL_DEFAULT_INTEREST,
        t1.GECIKME_ZAMMI_KDV    TL_DEFAULT_INTEREST_TAX,
        t1.KAPATMA_TARIHI       CLOSING_DATE,
        t1.KAPATMA_SAATI        CLOSING_TIME,
        t1.KAPATMA_VEZNEDAR     CLOSING_TELLERS,
        t1.KAPATMA_VEZNE        CLOSING_CASH_DESK,  
        t1.KAPATMA_SUBE         CLOSING_BRANCH,
        t1.KAPATMA_SEKLI        CLOSING_SHAPE,
        t1.KAPATMA_SEKLI_BANKA_KODU CLOSING_BANK_CODE_SHAPE,
        t1.AKIBET_DUR           STATUS_TYPE_ID,
        nvl2(t2.FATURA_NO,1,0)  IS_GOVERNMENT,
        t1.BAGLANTI_DUR         CONNECTION_TYPE,
        t1.KODLAMA_TARIHI       BANK_COLLECTION_DATE,
        t1.OG_DUR               VOLTAGE_TYPE,
        t1.TANZIM_TARIHI        ARRANGEMENT_DATE     
      FROM
        '||gv_stg_owner||'.'||v_src_table_01||' t1,
        (
          select /*+ parallel(s1,16) */ distinct 
            s1.FATURA_NO, 
            s1.FATURA_SERI 
          from
            '||gv_ods_mbs_dicle_owner ||'.'||v_src_table_02||' s1
        ) t2
      WHERE
        t1.FATURA_NO  = t2.FATURA_NO  (+) AND
        t1.FATURA_SERI= t2.FATURA_SERI(+) 
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    commit;

    <<END_PROC>>null;

    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;


  ------------------------------------------------------------------------------
  --  Created By     : Ceyhun Kerti
  --  Creation Date  : 2017.04.14
  --  Procedure Name : PRC_INV_FATURA_LOADER
  --  Description    : Eski tarihli kayitlarin yuklemesini baslatan proc. 
  --
  --  [Modification History]
  --  --------------------------------------------------------------------------
  PROCEDURE PRC_INV_FATURA_LOADER(
    pin_parallel  number default null
  )
  IS
    ----------------------------------------------------------------------------
    v_child_process_error EXCEPTION;
    PRAGMA EXCEPTION_INIT( v_child_process_error, -20001 );
    ----------------------------------------------------------------------------
    v_parallel      number := to_number(trim(fun_get_inv_param('FATURA_EXT_PARALLEL')));  
    v_rowid_query   long;
    v_rowid_cursor  rowid_cursor_type;
    v_rowid_from    varchar2(255);
    v_rowid_to      varchar2(255);
    v_thread_no     number := 1;
    v_job_action    varchar2(4000);
    v_sleep_sec     number := 5;
    v_has_error     varchar2(10) := '0';
  BEGIN

    gv_proc := 'PRC_INV_FATURA_LOADER';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
    
    -- override the value from the inv params table with the argument
    if pin_parallel is not null then 
      v_parallel := pin_parallel;
    end if;

    -- we don't need rowid ranges it is no-parallel
    -- skip the parts related with parallel processes
    if v_parallel < 2 then 
      PRC_INV_FATURA(pib_truncate=>true);
      goto END_PROC;
    end if;

    -- reset completed child count to zero
    prc_set_inv_param('FATURA_EXT_COMPLETED_PROCESS',0);

    -- reset child process error
    prc_set_inv_param('FATURA_EXT_CHILD_ERROR',0);
    

    -- get rowid query for given parallel limit
    v_rowid_query := fun_get_rowid_range_query(
      fin_parallel => v_parallel
    );


    open v_rowid_cursor for v_rowid_query;
    loop
      -- create an async job for each rowid-range
      fetch v_rowid_cursor into v_rowid_from,v_rowid_to;
      exit when v_rowid_cursor%notfound;
      
      -- script to execute in async-job
      v_job_action := '
        begin 
          dwh.pkg_inv_invoice.PRC_INV_FATURA(
            pin_part_num   => '  ||v_thread_no ||',
            piv_rowid_from => '''||v_rowid_from||''',     
            piv_rowid_to   => '''||v_rowid_to  ||'''
          ); 
        end;
      ';

      -- invoke the async-job
      dbms_scheduler.create_job (  
        job_name      =>  'PRC_INV_FATURA_'||v_thread_no,  
        job_type      =>  'PLSQL_BLOCK',  
        job_action    =>  v_job_action,  
        start_date    =>  sysdate,  
        enabled       =>  true,  
        auto_drop     =>  true,  
        comments      =>  '
          PRC_INV_FATURA_'||v_thread_no||' 
          from: '||v_rowid_from||'
          to: '  ||v_rowid_to  ||'
        ');   

      v_thread_no := v_thread_no + 1;

      plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,null,'PRC_INV_FATURA_'||v_thread_no);

    end loop;


    -- wait for processes to complete
    v_sleep_sec := to_number(trim(fun_get_inv_param('FATURA_EXT_SLEEP_SEC')));  
    loop
      sleep(v_sleep_sec*1000);
      v_has_error := fun_get_inv_param('FATURA_EXT_CHILD_ERROR');
      if v_has_error = '1' then
        raise_application_error( -20001, 'One of the childs has error. Kill others and restart!' );
      end if;
      exit when v_parallel <= to_number(trim(fun_get_inv_param('FATURA_EXT_COMPLETED_PROCESS')));
    end loop;


    <<END_PROC>>

    prc_set_inv_param('FATURA_EXT_STATUS',gv_ext_status_success);
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, null, NULL, gv_dyn_task);
        rollback;
        prc_set_inv_param('FATURA_EXT_STATUS',gv_ext_status_error);
        raise_application_error(gv_sql_errc, gv_sql_errm);
  END;


  ------------------------------------------------------------------------------
  --  Created By     : Ceyhun Kerti
  --  Creation Date  : 2017.04.14
  --  Procedure Name : PRC_INV_FATURA
  --  Description    : extracts data for the given ranges, this proc works as
  --                   a child for the main loader process.
  --
  --  [Modification History]
  --  --------------------------------------------------------------------------
  PROCEDURE PRC_INV_FATURA(
    pin_part_num    varchar2  default 1, 
    piv_rowid_from  varchar2  default null, 
    piv_rowid_to    varchar2  default null,
    pib_truncate    boolean   default false
  )
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'FATURA';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'FATURA';
    ----------------------------------------------------------------------------
    v_partition     varchar2(30) := 'P'||pin_part_num;
    ----------------------------------------------------------------------------
    v_db_link varchar2(4000) := fun_get_inv_param('FATURA_EXT_DBLK');
    v_active  varchar2(4000) := fun_get_inv_param('FATURA_EXT_ACTIVE');
    v_status  varchar2(4000) := fun_get_inv_param('FATURA_EXT_STATUS');
    v_select_hint varchar2(4000) := fun_get_inv_param('FATURA_EXT_SELECT_HINT');
    ----------------------------------------------------------------------------
    v_filter long := '';
    ----------------------------------------------------------------------------
    procedure inc_completed
    is
      v_completed_process number := 0;
    begin
      v_completed_process := to_number(fun_get_inv_param('FATURA_EXT_COMPLETED_PROCESS'));
      v_completed_process := v_completed_process + 1;
      prc_set_inv_param('FATURA_EXT_COMPLETED_PROCESS',v_completed_process);
    end;
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_INV_FATURA';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
    
    plib.truncate_partition(gv_ods_mbs_dicle_owner, v_table_name, v_partition);

    -- if no parallel
    if pib_truncate = true then
      plib.truncate_table(gv_ods_mbs_dicle_owner, v_table_name);
    end if;

    -- for parallel loads
    if piv_rowid_from is not null then
      v_filter := v_filter|| ' AND
        t1.rowid between '''|| piv_rowid_from||''' and '''|| piv_rowid_to||'''
      ';
    end if;
    

    gv_dyn_task := '
      INSERT /*+ append nologging */ INTO '|| gv_ods_mbs_dicle_owner ||'.'||v_table_name||' 
      PARTITION('||v_partition||')
      (
        PART_NUM,  
        FATURA_SERI,              
        FATURA_NO,                
        FATURA_TIPI,              
        TESISAT_NO,               
        SOZLESME_NO,              
        MUTA_YIL,                 
        MUTA_NO,                  
        ILK_OKUMA_TARIHI,         
        OKUMA_TARIHI,             
        TAHAKKUK_TARIHI,          
        TANZIM_TARIHI,            
        SON_ODEME_TARIHI,         
        UNVAN,                    
        TARIFE_KODU,
        AKTIF_KWH,                
        AKTIF_MIKTAR,             
        REAKTIF_MIKTAR,           
        SATICI_KWH,               
        DAGITIM_BEDELI,           
        TOPLAM_TUKETIM,           
        EE_FONU,                  
        TRT_PAYI,                 
        BELEDIYE_VERGISI,         
        TOPLAM_KDV,               
        DEVIR_YUVARLAMA,          
        FATURA_TUTAR,             
        TOPLAM_TUTAR,             
        GECIKME_ZAMMI_TUTAR,      
        GECIKME_ZAMMI_KDV,        
        AKIBET_DUR,               
        FATURA_KODU,              
        FATURA_NEDENI,            
        IPTAL_NEDENI,             
        BOLGE_KODU,               
        F_BOLGE_KODU,             
        OKUYUCU_KODU,             
        OKUMA_SAATI,              
        BAGLANTI_DUR,             
        OG_DUR,                   
        KAPATMA_TARIHI,           
        KAPATMA_VEZNE,            
        KAPATMA_VEZNEDAR,         
        KAPATMA_SAATI,            
        KAPATMA_SUBE,             
        KAPATMA_SEKLI,            
        KAPATMA_SEKLI_BANKA_KODU, 
        KODLAMA_TARIHI,           
        SIRKET_KODU,              
        TEDARIK_DUR         
      )
      SELECT 
        '||pin_part_num||' PART_NUM,
        t1.FATURA_SERI,              
        t1.FATURA_NO,                
        t1.FATURA_TIPI,              
        t1.TESISAT_NO,               
        t1.SOZLESME_NO,              
        t1.MUTA_YIL,                 
        t1.MUTA_NO,                  
        t1.ILK_OKUMA_TARIHI,         
        t1.OKUMA_TARIHI,             
        t1.TAHAKKUK_TARIHI,          
        t1.TANZIM_TARIHI,            
        t1.SON_ODEME_TARIHI,         
        t1.UNVAN,                    
        t1.TARIFE_KODU,
        t1.AKTIF_KWH,                
        t1.AKTIF_MIKTAR,             
        t1.REAKTIF_MIKTAR,           
        t1.SATICI_KWH,               
        t1.DAGITIM_BEDELI,           
        t1.TOPLAM_TUKETIM,           
        t1.EE_FONU,                  
        t1.TRT_PAYI,                 
        t1.BELEDIYE_VERGISI,         
        t1.TOPLAM_KDV,               
        t1.DEVIR_YUVARLAMA,          
        t1.FATURA_TUTAR,             
        t1.TOPLAM_TUTAR,             
        t1.GECIKME_ZAMMI_TUTAR,      
        t1.GECIKME_ZAMMI_KDV,        
        t1.AKIBET_DUR,               
        t1.FATURA_KODU,              
        t1.FATURA_NEDENI,            
        t1.IPTAL_NEDENI,             
        t1.BOLGE_KODU,               
        t1.F_BOLGE_KODU,             
        t1.OKUYUCU_KODU,             
        t1.OKUMA_SAATI,              
        t1.BAGLANTI_DUR,             
        t1.OG_DUR,                   
        t1.KAPATMA_TARIHI,           
        t1.KAPATMA_VEZNE,            
        t1.KAPATMA_VEZNEDAR,         
        t1.KAPATMA_SAATI,            
        t1.KAPATMA_SUBE,             
        t1.KAPATMA_SEKLI,            
        t1.KAPATMA_SEKLI_BANKA_KODU, 
        t1.KODLAMA_TARIHI,           
        t1.SIRKET_KODU,              
        t1.TEDARIK_DUR
      FROM
        '||gv_mbs_dicle_owner ||'.'||v_src_table_01||'@'||nvl(v_db_link, gv_dblk_mbs)||' t1
      WHERE
        (1=1)
        '||v_filter||'
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    commit;

    <<END_PROC>>       

    inc_completed;

    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        inc_completed;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;

  ------------------------------------------------------------------------------
  --  Created By     : Ceyhun Kerti
  --  Creation Date  : 2017.03.29
  --  Procedure Name : PRC_INV_INVOICE
  --  Description    : Load INVOICE table 
  --
  --  [Modification History]
  --  --------------------------------------------------------------------------
  PROCEDURE PRC_INV_INVOICE
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'INV_INVOICE';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'FATURA';
    v_src_table_02  varchar2(30) := 'TEMP_DEVIR_TEDAS';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_INV_INVOICE';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.truncate_table(gv_dwh_owner, v_table_name);

    gv_dyn_task := '
      INSERT /*+ append nologging */ INTO '|| gv_dwh_owner ||'.'||v_table_name||'
      (
        INVOICE_SERIAL,
        INVOICE_NUMBER,
        INSTALLATION_ID,
        INVOICE_TYPE,  
        INVOICE_STATUS,
        SUPPLY_SITUATION,
        INVOICE_CAUSE,
        COMPANY_CODE,
        CANCEL_CAUSE,
        NAME,
        CONTRACT_NUMBER,
        DISTRICT_CODE,
        I_DISTRICT_CODE,
        TARIFF_CODE,
        READER_CODE,
        READER_TIME,
        LEAKAGE_YEAR,
        LEAKAGE_NUMBER,
        INVOICE_DATE,
        FIRST_READ_DATE,
        LAST_READ_DATE,
        READ_DAY,
        LAST_PAYMENT_DATE,
        KWH_ACTIVE,
        KWH_ACTIVE_AMOUNT,
        KWH_SELLER,
        KWH_ADDITION,
        KWH_TOTAL,
        KWH_REAKTIF,
        TL_TOTAL_TAX,
        TL_DISTRIBUTION_COST,
        TL_EE_FUND,
        TL_TRT_SHARE,
        TL_COUNCIL_TAX,
        TL_TURNOVER_ROUNDING,
        TL_TOTAL_CONSUMPTION,
        TL_INVOICE_TOTAL,
        TL_GENERAL_TOTAL,
        TL_DEFAULT_INTEREST,
        TL_DEFAULT_INTEREST_TAX,
        CLOSING_DATE,
        CLOSING_TIME,
        CLOSING_TELLERS,
        CLOSING_CASH_DESK,
        CLOSING_BRANCH,
        CLOSING_SHAPE,
        CLOSING_BANK_CODE_SHAPE,
        STATUS_TYPE_ID,
        IS_GOVERNMENT,
        CONNECTION_TYPE,
        BANK_COLLECTION_DATE,
        VOLTAGE_TYPE,
        ARRANGEMENT_DATE
      )
      SELECT --+ parallel(t1, 16)
        t1.FATURA_SERI      INVOICE_SERIAL, 
        t1.FATURA_NO        INVOICE_NUMBER, 
        t1.TESISAT_NO       INSTALLATION_ID,
        t1.FATURA_TIPI      INVOICE_TYPE,   
        t1.FATURA_KODU      INVOICE_STATUS, 
        t1.TEDARIK_DUR      SUPPLY_SITUATION,
        t1.FATURA_NEDENI    INVOICE_CAUSE,
        t1.SIRKET_KODU      COMPANY_CODE,        
        t1.IPTAL_NEDENI     CANCEL_CAUSE,
        t1.UNVAN            NAME,
        t1.SOZLESME_NO      CONTRACT_NUMBER,
        t1.BOLGE_KODU       DISTRICT_CODE,
        t1.F_BOLGE_KODU     I_DISTRICT_CODE,
        t1.TARIFE_KODU      TARIFF_CODE,
        t1.OKUYUCU_KODU     READER_CODE,
        t1.OKUMA_SAATI      READER_TIME,
        t1.MUTA_YIL         LEAKAGE_YEAR,
        t1.MUTA_NO          LEAKAGE_NUMBER,
        t1.TAHAKKUK_TARIHI  INVOICE_DATE,
        t1.ILK_OKUMA_TARIHI FIRST_READ_DATE,
        t1.OKUMA_TARIHI     LAST_READ_DATE,
        ------------------------------------------------------------------------
        case 
          when t1.OKUMA_TARIHI is not null and t1.ILK_OKUMA_TARIHI is not null
          then t1.OKUMA_TARIHI - t1.ILK_OKUMA_TARIHI
          else 0
        end READ_DAY,
        ------------------------------------------------------------------------
        t1.SON_ODEME_TARIHI LAST_PAYMENT_DATE,
        t1.AKTIF_KWH        KWH_ACTIVE,
        t1.AKTIF_MIKTAR     KWH_ACTIVE_AMOUNT,
        t1.SATICI_KWH       KWH_SELLER,
        0                   KWH_ADDITION,
        ------------------------------------------------------------------------
        nvl2(t1.SATICI_KWH,AKTIF_KWH,t1.SATICI_KWH) KWH_TOTAL,
        ------------------------------------------------------------------------
        t1.REAKTIF_MIKTAR       KWH_REAKTIF,
        t1.TOPLAM_KDV           TL_TOTAL_TAX,
        t1.DAGITIM_BEDELI       TL_DISTRIBUTION_COST,
        t1.EE_FONU              TL_EE_FUND,
        t1.TRT_PAYI             TL_TRT_SHARE,
        t1.BELEDIYE_VERGISI     TL_COUNCIL_TAX,
        t1.DEVIR_YUVARLAMA      TL_TURNOVER_ROUNDING,
        t1.TOPLAM_TUKETIM       TL_TOTAL_CONSUMPTION,
        t1.FATURA_TUTAR         TL_INVOICE_TOTAL,
        t1.TOPLAM_TUTAR         TL_GENERAL_TOTAL,
        t1.GECIKME_ZAMMI_TUTAR  TL_DEFAULT_INTEREST,
        t1.GECIKME_ZAMMI_KDV    TL_DEFAULT_INTEREST_TAX,
        t1.KAPATMA_TARIHI       CLOSING_DATE,
        t1.KAPATMA_SAATI        CLOSING_TIME,
        t1.KAPATMA_VEZNEDAR     CLOSING_TELLERS,
        t1.KAPATMA_VEZNE        CLOSING_CASH_DESK,  
        t1.KAPATMA_SUBE         CLOSING_BRANCH,
        t1.KAPATMA_SEKLI        CLOSING_SHAPE,
        t1.KAPATMA_SEKLI_BANKA_KODU CLOSING_BANK_CODE_SHAPE,
        t1.AKIBET_DUR           STATUS_TYPE_ID,
        nvl2(t2.FATURA_NO,1,0)  IS_GOVERNMENT,
        t1.BAGLANTI_DUR         CONNECTION_TYPE,
        t1.KODLAMA_TARIHI       BANK_COLLECTION_DATE,
        t1.OG_DUR               VOLTAGE_TYPE,
        t1.TANZIM_TARIHI        ARRANGEMENT_DATE     
      FROM
        '||gv_ods_mbs_dicle_owner||'.'||v_src_table_01||' t1,
        (
          select /*+ parallel(s1,16) */ distinct 
            s1.FATURA_NO, 
            s1.FATURA_SERI 
          from
            '||gv_ods_mbs_dicle_owner ||'.'||v_src_table_02||' s1
        ) t2
      WHERE
        t1.FATURA_NO  = t2.FATURA_NO  (+) AND
        t1.FATURA_SERI= t2.FATURA_SERI(+)
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;

  ------------------------------------------------------------------------------
  --  Created By     : Ceyhun Kerti
  --  Creation Date  : 2017.04.05
  --  Procedure Name : PRC_TINV0000_ABONMAN_FATURA
  --  Description    :  
  --
  --  [Modification History]
  --  --------------------------------------------------------------------------
  PROCEDURE PRC_TINV0000_ABONMAN_FATURA
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TINV0000_ABONMAN_FATURA';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'ABONMAN_FATURA';
    ----------------------------------------------------------------------------
    v_db_link varchar2(4000) := fun_get_inv_param('FATURA_EXT_DBLK');
    ----------------------------------------------------------------------------
  BEGIN
    
    gv_proc := 'PRC_TINV0000_ABONMAN_FATURA';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_dwh_owner,v_table_name);

    gv_dyn_task := '
      CREATE TABLE '|| gv_stg_owner ||'.'||v_table_name||'
      PARALLEL NOLOGGING COMPRESS
      AS
      SELECT --+ parallel(t1, 16)
        t1.FATURA_SERI             ,  
        t1.FATURA_NO               ,  
        t1.SIRA_NO                 ,  
        t1.TESISAT_NO              ,  
        t1.TALEP_BOLGE             ,  
        t1.TALEP_YIL               ,  
        t1.TALEP_NO                ,  
        t1.ISEMRI_YIL              ,  
        t1.ISEMRI_BOLGE            ,  
        t1.ISEMRI_DEFTER           ,  
        t1.ISEMRI_SIRA             ,  
        t1.MUTA_YIL                ,  
        t1.MUTA_NO                 ,  
        t1.SOZLESME_NO             ,  
        t1.HUKUK_EVRAK_NO          ,  
        t1.BEDEL_KODU              ,  
        t1.KAPATMA_TARIHI          ,  
        t1.KAPATMA_VEZNE           ,  
        t1.KAPATMA_VEZNEDAR        ,  
        t1.TUTAR                   ,  
        t1.VADE_FARKI_TUTAR        ,  
        t1.BELEDIYE_VERGISI        ,  
        t1.TOPLAM_KDV              ,  
        t1.TOPLAM_AVANS            ,  
        t1.TOPLAM_TUTAR            ,  
        t1.IPTAL_TARIHI            ,  
        t1.HESAP_KODU              ,  
        t1.IADE_TARIHI             ,  
        t1.IADE_TAKSIT_NO          ,  
        t1.KAPATMA_SEKLI           ,  
        t1.F_YIL                   ,  
        t1.F_FATURA_SERI           ,  
        t1.F_FATURA_NO             ,  
        t1.BOLGE_KODU              ,  
        t1.KAPATMA_SAATI           ,  
        t1.AT_SIRA_NO              ,  
        t1.AT_TAKSIT_NO            ,  
        t1.K_MAKBUZ_SERI           ,  
        t1.K_MAKBUZ_NO             ,  
        t1.KAPATMA_SEKLI_BANKA_KODU,  
        t1.DAMGA_MAHSUP            ,  
        t1.BASIM_TARIHI            ,  
        t1.KAPATMA_SUBE            ,  
        t1.VD_KODU                 ,  
        t1.VERGI_NO                ,  
        t1.F_KAPATMA_TARIHI        ,  
        t1.SIRKET_KODU             ,  
        t1.TC_KIMLIK_NO              
      FROM  
        '||gv_mbs_dicle_owner ||'.'||v_src_table_01||'@'||nvl(v_db_link, gv_dblk_mbs)||' t1  
      WHERE
        t1.KAPATMA_TARIHI is not null and 
        t1.KAPATMA_TARIHI = to_number(to_char(sysdate,''yyyymmdd'')) 
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    

    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;


  ------------------------------------------------------------------------------
  --  Created By     : Ceyhun Kerti
  --  Creation Date  : 2017.04.05
  --  Procedure Name : PRC_INV_SUBS_INVOICE_LIVE
  --  Description    :  
  --
  --  [Modification History]
  --  --------------------------------------------------------------------------
  PROCEDURE PRC_INV_SUBS_INVOICE_LIVE
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'INV_SUBS_INVOICE_LIVE';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'TINV0000_ABONMAN_FATURA';
    v_src_table_02  varchar2(30) := 'TEMP_DEVIR_TEDAS';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_INV_SUBS_INVOICE_LIVE';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    
    ----------------------------------------------------------------------------
    gv_dyn_task := 'DELETE FROM '||gv_dwh_owner||'.'||v_table_name;
    execute immediate gv_dyn_task;
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    ----------------------------------------------------------------------------

    gv_dyn_task := '
      INSERT /*+ append nologging */ INTO '|| gv_dwh_owner ||'.'||v_table_name||'
      (
        INVOICE_SERIAL            ,
        INVOICE_NUMBER            ,
        SERIAL_ID                 ,
        INSTALLATION_ID           ,
        REQUEST_DISTRICT          ,
        REQUEST_YEAR              ,
        REQUEST_ID                ,
        LEAKAGE_YEAR              ,
        LEAKAGE_NO                ,
        CONTRACT_NUMBER           ,
        LEGAL_DOCUMENT_ID         ,
        ITEM_CODE                 ,
        CLOSING_DATE              ,
        CLOSING_CASH_DESK         ,
        CLOSING_TELLERS           ,
        TL_TOTAL                  ,
        TL_INTERESET_AMOUNT       ,
        MUNICIPALITY_TAX          ,
        TL_TOTAL_TAX              ,
        TL_GENERAL_TOTAL          ,
        CANCEL_DATE               ,
        ACCOUNT_CODE              ,
        GIVING_BACK_DATE          ,
        GIVING_BACK_INSTALLMENT_ID,
        CLOSING_SHAPE             ,
        F_YEAR                    ,
        F_INVOICE_SERIAL          ,
        F_INVOICE_NUMBER          ,
        DISTRICT_CODE             ,
        CLOSING_TIME              ,
        INSTALLMENT_SERIAL_ID     ,
        INSTALLMENT_NO            ,
        CLOSURE_RECEIPT_SERIAL    ,
        CLOSURE_RECEIPT_ID        ,
        CLOSING_BANK_CODE_SHAPE   ,
        TL_STAMP_TOTAL            ,
        CLOSING_BRANCH            ,
        TAX_OFFICE_CODE           ,
        TAX_OFFICE_NO             ,
        F_CLOSING_DATE            ,
        COMPANY_CODE              ,
        IS_GOVERNMENT             
      )
      SELECT --+ parallel(t1, 16) parallel(t2,16) use_hash(t1 t2)
        t1.FATURA_SERI        INVOICE_SERIAL,
        t1.FATURA_NO          INVOICE_NUMBER,
        t1.SIRA_NO            SERIAL_ID,
        t1.TESISAT_NO         INSTALLATION_ID,
        t1.TALEP_BOLGE        REQUEST_DISTRICT,
        t1.TALEP_YIL          REQUEST_YEAR,
        t1.TALEP_NO           REQUEST_ID,
        t1.MUTA_YIL           LEAKAGE_YEAR,
        t1.MUTA_NO            LEAKAGE_NO,
        t1.SOZLESME_NO        CONTRACT_NUMBER,
        t1.HUKUK_EVRAK_NO     LEGAL_DOCUMENT_ID,
        t1.BEDEL_KODU         ITEM_CODE,
        t1.KAPATMA_TARIHI     CLOSING_DATE,
        t1.KAPATMA_VEZNE      CLOSING_CASH_DESK,
        t1.KAPATMA_VEZNEDAR   CLOSING_TELLERS,
        t1.TUTAR              TL_TOTAL,
        t1.VADE_FARKI_TUTAR   TL_INTERESET_AMOUNT,
        t1.BELEDIYE_VERGISI   MUNICIPALITY_TAX,
        t1.TOPLAM_KDV         TL_TOTAL_TAX,
        t1.TOPLAM_TUTAR       TL_GENERAL_TOTAL,
        t1.IPTAL_TARIHI       CANCEL_DATE,
        t1.HESAP_KODU         ACCOUNT_CODE,
        t1.IADE_TARIHI        GIVING_BACK_DATE,
        t1.IADE_TAKSIT_NO     GIVING_BACK_INSTALLMENT_ID,
        t1.KAPATMA_SEKLI      CLOSING_SHAPE,
        t1.F_YIL              F_YEAR,
        t1.F_FATURA_SERI      F_INVOICE_SERIAL,
        t1.F_FATURA_NO        F_INVOICE_NUMBER,
        t1.BOLGE_KODU         DISTRICT_CODE,
        t1.KAPATMA_SAATI      CLOSING_TIME,
        t1.AT_SIRA_NO         INSTALLMENT_SERIAL_ID,
        t1.AT_TAKSIT_NO       INSTALLMENT_NO,
        t1.K_MAKBUZ_SERI      CLOSURE_RECEIPT_SERIAL,
        t1.K_MAKBUZ_NO        CLOSURE_RECEIPT_ID,
        t1.KAPATMA_SEKLI_BANKA_KODU CLOSING_BANK_CODE_SHAPE,
        t1.DAMGA_MAHSUP       TL_STAMP_TOTAL,
        t1.KAPATMA_SUBE       CLOSING_BRANCH,
        t1.VD_KODU            TAX_OFFICE_CODE,
        t1.VERGI_NO           TAX_OFFICE_NO,
        t1.F_KAPATMA_TARIHI   F_CLOSING_DATE,
        t1.SIRKET_KODU        COMPANY_CODE,
        nvl2(t2.AT_SIRA_NO,1,0) IS_GOVERNMENT
      FROM
        '||gv_stg_owner||'.'||v_src_table_01||' t1,
        (
          select /*+ parallel(s1,16) */ distinct 
            s1.AT_SIRA_NO
          from
            '||gv_ods_mbs_dicle_owner ||'.'||v_src_table_02||' s1
        ) t2
      WHERE
        t1.AT_SIRA_NO = t2.AT_SIRA_NO (+)
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;



  
  ------------------------------------------------------------------------------
  --  Created By     : Ceyhun Kerti
  --  Creation Date  : 2017.04.05
  --  Procedure Name : PRC_INV_SUBS_INVOICE
  --  Description    :  
  --
  --  [Modification History]
  --  --------------------------------------------------------------------------
  PROCEDURE PRC_INV_SUBS_INVOICE
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'INV_SUBS_INVOICE';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'ABONMAN_FATURA';
    v_src_table_02  varchar2(30) := 'TEMP_DEVIR_TEDAS';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_INV_SUBS_INVOICE';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.truncate_table(gv_dwh_owner, v_table_name);

    gv_dyn_task := '
      INSERT /*+ append nologging */ INTO '|| gv_dwh_owner ||'.'||v_table_name||'
      (
        INVOICE_SERIAL            ,
        INVOICE_NUMBER            ,
        SERIAL_ID                 ,
        INSTALLATION_ID           ,
        REQUEST_DISTRICT          ,
        REQUEST_YEAR              ,
        REQUEST_ID                ,
        LEAKAGE_YEAR              ,
        LEAKAGE_NO                ,
        CONTRACT_NUMBER           ,
        LEGAL_DOCUMENT_ID         ,
        ITEM_CODE                 ,
        CLOSING_DATE              ,
        CLOSING_CASH_DESK         ,
        CLOSING_TELLERS           ,
        TL_TOTAL                  ,
        TL_INTERESET_AMOUNT       ,
        MUNICIPALITY_TAX          ,
        TL_TOTAL_TAX              ,
        TL_GENERAL_TOTAL          ,
        CANCEL_DATE               ,
        ACCOUNT_CODE              ,
        GIVING_BACK_DATE          ,
        GIVING_BACK_INSTALLMENT_ID,
        CLOSING_SHAPE             ,
        F_YEAR                    ,
        F_INVOICE_SERIAL          ,
        F_INVOICE_NUMBER          ,
        DISTRICT_CODE             ,
        CLOSING_TIME              ,
        INSTALLMENT_SERIAL_ID     ,
        INSTALLMENT_NO            ,
        CLOSURE_RECEIPT_SERIAL    ,
        CLOSURE_RECEIPT_ID        ,
        CLOSING_BANK_CODE_SHAPE   ,
        TL_STAMP_TOTAL            ,
        CLOSING_BRANCH            ,
        TAX_OFFICE_CODE           ,
        TAX_OFFICE_NO             ,
        F_CLOSING_DATE            ,
        COMPANY_CODE              ,
        IS_GOVERNMENT             
      )
      SELECT --+ parallel(t1, 16) parallel(t2,16) use_hash(t1 t2)
        t1.FATURA_SERI        INVOICE_SERIAL,
        t1.FATURA_NO          INVOICE_NUMBER,
        t1.SIRA_NO            SERIAL_ID,
        t1.TESISAT_NO         INSTALLATION_ID,
        t1.TALEP_BOLGE        REQUEST_DISTRICT,
        t1.TALEP_YIL          REQUEST_YEAR,
        t1.TALEP_NO           REQUEST_ID,
        t1.MUTA_YIL           LEAKAGE_YEAR,
        t1.MUTA_NO            LEAKAGE_NO,
        t1.SOZLESME_NO        CONTRACT_NUMBER,
        t1.HUKUK_EVRAK_NO     LEGAL_DOCUMENT_ID,
        t1.BEDEL_KODU         ITEM_CODE,
        t1.KAPATMA_TARIHI     CLOSING_DATE,
        t1.KAPATMA_VEZNE      CLOSING_CASH_DESK,
        t1.KAPATMA_VEZNEDAR   CLOSING_TELLERS,
        t1.TUTAR              TL_TOTAL,
        t1.VADE_FARKI_TUTAR   TL_INTERESET_AMOUNT,
        t1.BELEDIYE_VERGISI   MUNICIPALITY_TAX,
        t1.TOPLAM_KDV         TL_TOTAL_TAX,
        t1.TOPLAM_TUTAR       TL_GENERAL_TOTAL,
        t1.IPTAL_TARIHI       CANCEL_DATE,
        t1.HESAP_KODU         ACCOUNT_CODE,
        t1.IADE_TARIHI        GIVING_BACK_DATE,
        t1.IADE_TAKSIT_NO     GIVING_BACK_INSTALLMENT_ID,
        t1.KAPATMA_SEKLI      CLOSING_SHAPE,
        t1.F_YIL              F_YEAR,
        t1.F_FATURA_SERI      F_INVOICE_SERIAL,
        t1.F_FATURA_NO        F_INVOICE_NUMBER,
        t1.BOLGE_KODU         DISTRICT_CODE,
        t1.KAPATMA_SAATI      CLOSING_TIME,
        t1.AT_SIRA_NO         INSTALLMENT_SERIAL_ID,
        t1.AT_TAKSIT_NO       INSTALLMENT_NO,
        t1.K_MAKBUZ_SERI      CLOSURE_RECEIPT_SERIAL,
        t1.K_MAKBUZ_NO        CLOSURE_RECEIPT_ID,
        t1.KAPATMA_SEKLI_BANKA_KODU CLOSING_BANK_CODE_SHAPE,
        t1.DAMGA_MAHSUP       TL_STAMP_TOTAL,
        t1.KAPATMA_SUBE       CLOSING_BRANCH,
        t1.VD_KODU            TAX_OFFICE_CODE,
        t1.VERGI_NO           TAX_OFFICE_NO,
        t1.F_KAPATMA_TARIHI   F_CLOSING_DATE,
        t1.SIRKET_KODU        COMPANY_CODE,
        nvl2(t2.AT_SIRA_NO,1,0) IS_GOVERNMENT
      FROM
        '||gv_ods_mbs_dicle_owner||'.'||v_src_table_01||' t1,
        (
          select /*+ parallel(s1,16) */ distinct 
            s1.AT_SIRA_NO
          from
            '||gv_ods_mbs_dicle_owner ||'.'||v_src_table_02||' s1
        ) t2
      WHERE
        t1.AT_SIRA_NO = t2.AT_SIRA_NO (+)
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;

  ------------------------------------------------------------------------------
  --  Created By     : Ceyhun Kerti
  --  Creation Date  : 2017.04.27
  --  Procedure Name : PRC_TINV0000_ABONMAN_TAKSIT
  --  Description    :  
  --
  --  [Modification History]
  --  --------------------------------------------------------------------------
  PROCEDURE PRC_TINV0000_ABONMAN_TAKSIT
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TINV0000_ABONMAN_TAKSIT';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'ABONMAN_TAKSIT';
    ----------------------------------------------------------------------------
    v_db_link varchar2(4000) := fun_get_inv_param('FATURA_EXT_DBLK');
    ----------------------------------------------------------------------------
  BEGIN
    
    gv_proc := 'PRC_TINV0000_ABONMAN_TAKSIT';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_dwh_owner,v_table_name);

    gv_dyn_task := '
      CREATE TABLE '|| gv_stg_owner ||'.'||v_table_name||'
      PARALLEL NOLOGGING COMPRESS
      AS
      SELECT --+ parallel(t1, 16)
        t1.SIRA_NO            ,
        t1.TAKSIT_NO          ,
        t1.TESISAT_NO         ,
        t1.MUTA_YIL           ,
        t1.MUTA_NO            ,
        t1.BEDEL_KODU         ,
        t1.KAYIT_TARIHI       ,
        t1.VADE_TARIHI        ,
        t1.SAHIS_DUR          ,
        t1.UNVAN              ,
        t1.SOYAD              ,
        t1.TUTAR              ,
        t1.BELEDIYE_VERGISI   ,
        t1.VADE_FARKI_TUTAR   ,
        t1.TOPLAM_KDV         ,
        t1.TOPLAM_TUTAR       ,
        t1.GECIKME_ZAMMI_TUTAR,
        t1.GECIKME_ZAMMI_KDV  ,
        t1.KAPATMA_TARIHI     ,
        t1.KAPATMA_VEZNE      ,
        t1.KAPATMA_VEZNEDAR   ,
        t1.FATURA_KODU        ,
        t1.MAKBUZ_SERI        ,
        t1.MAKBUZ_NO          ,
        t1.MAKBUZ_SIRA_NO     ,
        t1.REF_MAKBUZ_SERI    ,
        t1.REF_MAKBUZ_NO      ,
        t1.IPTAL_TARIHI       ,
        t1.HUKUK_DUR          ,
        t1.AKIBET_DUR         ,
        t1.KODLAMA_TARIHI     ,
        t1.BOLGE_KODU         ,
        t1.TARIFE_KODU        ,
        t1.DAMGA_MAHSUP       ,
        t1.ODEME_SIRA_NO      ,
        t1.SIRKET_KODU        
      FROM  
        '||gv_mbs_dicle_owner ||'.'||v_src_table_01||'@'||nvl(v_db_link, gv_dblk_mbs)||' t1  
      WHERE
        t1.KAPATMA_TARIHI is not null and 
        t1.KAPATMA_TARIHI = to_number(to_char(sysdate,''yyyymmdd'')) 
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    

    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;

  ------------------------------------------------------------------------------
  --  Created By     : Ceyhun Kerti
  --  Creation Date  : 2017.04.27
  --  Procedure Name : PRC_INV_SUBS_INSTALLMENT_LIVE
  --  Description    :  
  --
  --  [Modification History]
  --  --------------------------------------------------------------------------
  PROCEDURE PRC_INV_SUBS_INSTALLMENT_LIVE
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'INV_SUBS_INSTALLMENT_LIVE';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'TINV0000_ABONMAN_TAKSIT';
    v_src_table_02  varchar2(30) := 'TEMP_DEVIR_TEDAS';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_INV_SUBS_INSTALLMENT';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    ----------------------------------------------------------------------------
    gv_dyn_task := 'DELETE FROM '||gv_dwh_owner||'.'||v_table_name;
    execute immediate gv_dyn_task;
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    ----------------------------------------------------------------------------

    gv_dyn_task := '
      INSERT /*+ append nologging */ INTO '|| gv_dwh_owner ||'.'||v_table_name||'
      (
        SERIAL_ID,
        INSTALLMENT_NO,
        INSTALLATION_ID,    
        LEAKAGE_YEAR,       
        LEAKAGE_NUMBER,     
        ITEM_CODE,          
        CREATE_DATE,          
        DUE_DATE,             
        NAME,                   
        TL_TOTAL,               
        TL_INTEREST_AMOUNT,     
        TL_TOTAL_TAX,           
        TL_GENERAL_TOTAL,       
        TL_DEFAULT_INTEREST,      
        TL_DEFAULT_INTEREST_TAX,  
        CLOSING_DATE,           
        CLOSING_CASH_DESK,      
        CLOSING_TELLERS,        
        INVOICE_STATUS,         
        CANCEL_DATE,            
        STATUS_TYPE_ID,         
        BANK_COLLECTION_DATE,     
        DISTRICT_CODE,            
        TARIFF_CODE,              
        COMPANY_CODE,           
        IS_GOVERNMENT           
      )
      SELECT --+ parallel(t1, 16) parallel(t2,16) use_hash(t1 t2)
        t1.SIRA_NO              SERIAL_ID,
        t1.TAKSIT_NO            INSTALLMENT_NO,
        t1.TESISAT_NO           INSTALLATION_ID,    
        t1.MUTA_YIL             LEAKAGE_YEAR,       
        t1.MUTA_NO              LEAKAGE_NUMBER,     
        t1.BEDEL_KODU           ITEM_CODE,          
        t1.KAYIT_TARIHI         CREATE_DATE,          
        t1.VADE_TARIHI          DUE_DATE,             
        t1.UNVAN                NAME,                   
        t1.TUTAR                TL_TOTAL,               
        t1.VADE_FARKI_TUTAR     TL_INTEREST_AMOUNT,     
        t1.TOPLAM_KDV           TL_TOTAL_TAX,           
        t1.TOPLAM_TUTAR         TL_GENERAL_TOTAL,       
        t1.GECIKME_ZAMMI_TUTAR  TL_DEFAULT_INTEREST,      
        t1.GECIKME_ZAMMI_KDV    TL_DEFAULT_INTEREST_TAX,  
        t1.KAPATMA_TARIHI       CLOSING_DATE,           
        t1.KAPATMA_VEZNE        CLOSING_CASH_DESK,      
        t1.KAPATMA_VEZNEDAR     CLOSING_TELLERS,        
        t1.FATURA_KODU          INVOICE_STATUS,         
        t1.IPTAL_TARIHI         CANCEL_DATE,            
        t1.AKIBET_DUR           STATUS_TYPE_ID,         
        t1.KODLAMA_TARIHI       BANK_COLLECTION_DATE,     
        t1.BOLGE_KODU           DISTRICT_CODE,            
        t1.TARIFE_KODU          TARIFF_CODE,              
        t1.SIRKET_KODU          COMPANY_CODE,           
        nvl2(t2.AT_SIRA_NO,1,0) IS_GOVERNMENT           
      FROM
        '||gv_stg_owner||'.'||v_src_table_01||' t1,
        (
          select /*+ parallel(s1,16) */ distinct 
            s1.AT_SIRA_NO
          from
            '||gv_ods_mbs_dicle_owner ||'.'||v_src_table_02||' s1
        ) t2
      WHERE
        t1.SIRA_NO = t2.AT_SIRA_NO (+)
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;



  ------------------------------------------------------------------------------
  --  Created By     : Ceyhun Kerti
  --  Creation Date  : 2017.04.05
  --  Procedure Name : PRC_INV_SUBS_INSTALLMENT
  --  Description    :  
  --
  --  [Modification History]
  --  --------------------------------------------------------------------------
  PROCEDURE PRC_INV_SUBS_INSTALLMENT
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'INV_SUBS_INSTALLMENT';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'ABONMAN_TAKSIT';
    v_src_table_02  varchar2(30) := 'TEMP_DEVIR_TEDAS';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_INV_SUBS_INSTALLMENT';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.truncate_table(gv_dwh_owner, v_table_name);

    gv_dyn_task := '
      INSERT /*+ append nologging */ INTO '|| gv_dwh_owner ||'.'||v_table_name||'
      (
        SERIAL_ID,
        INSTALLMENT_NO,
        INSTALLATION_ID,    
        LEAKAGE_YEAR,       
        LEAKAGE_NUMBER,     
        ITEM_CODE,          
        CREATE_DATE,          
        DUE_DATE,             
        NAME,                   
        TL_TOTAL,               
        TL_INTEREST_AMOUNT,     
        TL_TOTAL_TAX,           
        TL_GENERAL_TOTAL,       
        TL_DEFAULT_INTEREST,      
        TL_DEFAULT_INTEREST_TAX,  
        CLOSING_DATE,           
        CLOSING_CASH_DESK,      
        CLOSING_TELLERS,        
        INVOICE_STATUS,         
        CANCEL_DATE,            
        STATUS_TYPE_ID,         
        BANK_COLLECTION_DATE,     
        DISTRICT_CODE,            
        TARIFF_CODE,              
        COMPANY_CODE,           
        IS_GOVERNMENT           
      )
      SELECT --+ parallel(t1, 16) parallel(t2,16) use_hash(t1 t2)
        t1.SIRA_NO              SERIAL_ID,
        t1.TAKSIT_NO            INSTALLMENT_NO,
        t1.TESISAT_NO           INSTALLATION_ID,    
        t1.MUTA_YIL             LEAKAGE_YEAR,       
        t1.MUTA_NO              LEAKAGE_NUMBER,     
        t1.BEDEL_KODU           ITEM_CODE,          
        t1.KAYIT_TARIHI         CREATE_DATE,          
        t1.VADE_TARIHI          DUE_DATE,             
        t1.UNVAN                NAME,                   
        t1.TUTAR                TL_TOTAL,               
        t1.VADE_FARKI_TUTAR     TL_INTEREST_AMOUNT,     
        t1.TOPLAM_KDV           TL_TOTAL_TAX,           
        t1.TOPLAM_TUTAR         TL_GENERAL_TOTAL,       
        t1.GECIKME_ZAMMI_TUTAR  TL_DEFAULT_INTEREST,      
        t1.GECIKME_ZAMMI_KDV    TL_DEFAULT_INTEREST_TAX,  
        t1.KAPATMA_TARIHI       CLOSING_DATE,           
        t1.KAPATMA_VEZNE        CLOSING_CASH_DESK,      
        t1.KAPATMA_VEZNEDAR     CLOSING_TELLERS,        
        t1.FATURA_KODU          INVOICE_STATUS,         
        t1.IPTAL_TARIHI         CANCEL_DATE,            
        t1.AKIBET_DUR           STATUS_TYPE_ID,         
        t1.KODLAMA_TARIHI       BANK_COLLECTION_DATE,     
        t1.BOLGE_KODU           DISTRICT_CODE,            
        t1.TARIFE_KODU          TARIFF_CODE,              
        t1.SIRKET_KODU          COMPANY_CODE,           
        nvl2(t2.AT_SIRA_NO,1,0) IS_GOVERNMENT           
      FROM
        '||gv_ods_mbs_dicle_owner||'.'||v_src_table_01||' t1,
        (
          select /*+ parallel(s1,16) */ distinct 
            s1.AT_SIRA_NO
          from
            '||gv_ods_mbs_dicle_owner ||'.'||v_src_table_02||' s1
        ) t2
      WHERE
        t1.SIRA_NO = t2.AT_SIRA_NO (+)
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;

  ------------------------------------------------------------------------------
  --  Created By     : Ceyhun Kerti
  --  Creation Date  : 2017.04.27
  --  Procedure Name : PRC_INV_INVOICE_DETAIL_LIVE
  --  Description    :  
  --
  --  [Modification History]
  --  --------------------------------------------------------------------------
  PROCEDURE PRC_INV_INVOICE_DETAIL_LIVE
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'INV_INVOICE_DETAIL_LIVE';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'INV_INVOICE_LIVE';
    v_src_table_02  varchar2(30) := 'D_SUBSCRIBERS';
    v_src_table_03  varchar2(30) := 'D_ACCOUNTING_ACCOUNTS';
    v_src_table_04  varchar2(30) := 'D_TARIFFS';
    v_src_table_05  varchar2(30) := 'D_CITIES';
    v_src_table_06  varchar2(30) := 'D_REGIONS';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_INV_INVOICE_DETAIL_LIVE';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    
    ----------------------------------------------------------------------------
    gv_dyn_task := 'DELETE FROM '||gv_dwh_owner||'.'||v_table_name;
    execute immediate gv_dyn_task;
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    ----------------------------------------------------------------------------

    gv_dyn_task := '
      INSERT /*+ append nologging */ INTO '|| gv_dwh_owner ||'.'||v_table_name||'
      (
        INVOICE_DATE,
        COMPANY_CODE,
        SUBSCRIBER_GROUP_NAME,
        CITY_NAME,
        INVOICE_TYPE,
        STATUS_TYPE_ID,
        SUBSCRIBER_KIND,
        SUPPLY_SITUATION,
        KWH_ACTIVE,
        TL_GENERAL_TOTAL
      )
      SELECT --+ parallel(q1, 16)
        q1.INVOICE_DATE,
        q1.COMPANY_CODE,
        q1.SUBSCRIBER_GROUP_NAME,
        q1.CITY_NAME,
        q1.INVOICE_TYPE,
        q1.STATUS_TYPE_ID,
        q1.SUBSCRIBER_KIND,
        q1.SUPPLY_SITUATION,
        ------------------------------------------------------------------------  
        sum(q1.KWH_ACTIVE) KWH_ACTIVE,
        ------------------------------------------------------------------------
        sum(q1.TL_GENERAL_TOTAL) TL_GENERAL_TOTAL
        ------------------------------------------------------------------------
      FROM
        (
          select 
            /*+
              parallel(t1, 16) parallel(t2,16) parallel(t3,16) 
              use_hash(t1 t2 t3 t4 t5 t6)
            */
            decode(t1.COMPANY_CODE,1,''Dedaş'',''Depsaş'')  COMPANY_CODE,
            to_date(t1.INVOICE_DATE, ''yyyymmdd'')          INVOICE_DATE,
            ------------------------------------------------------------------------
            case
              when nvl(t1.INSTALLATION_ID, 0) = 0 and nvl(t4.TARIFF_CODE, 0) > 0
              then trim(nvl(t3.SUBSCRIBER_GROUP_NAME, ''TANIMSIZ''))
              else trim(t2.SUBSCRIBER_GROUP_NAME)
            end SUBSCRIBER_GROUP_NAME, 
            ------------------------------------------------------------------------
            trim(t5.CITY_NAME) CITY_NAME,
            ------------------------------------------------------------------------
            case
              when t1.INVOICE_TYPE = 1 THEN ''Z Fatura''                    -- Z Fatura
              when t1.INVOICE_TYPE = 5 THEN ''Y Fatura''                    -- Y Fatura
              when t1.INVOICE_TYPE = 4 THEN ''Ek Tahakkuk''                 -- Ek Tahakkuk
              else ''Normal''                                               -- Normal
            end INVOICE_TYPE,
            ------------------------------------------------------------------------    
            case
              when t1.STATUS_TYPE_ID = 3 THEN ''Borç Takibi - g''           -- Borç Takibi - g
              when t1.STATUS_TYPE_ID = 7 THEN ''İcra Uygun - h''            -- İcra Uygun - h
              when t1.STATUS_TYPE_ID = 8 THEN ''İcra - H''                  -- İcra - H
              else ''Normal''                                               -- Normal
            end STATUS_TYPE_ID,
            ------------------------------------------------------------------------
            t2.SUBSCRIBER_KIND,
            ------------------------------------------------------------------------
            case
              when coalesce(t1.SUPPLY_SITUATION, 0) = 0 then ''Normal''
              when coalesce(t1.SUPPLY_SITUATION, 0) = 1 then ''K1''
              when coalesce(t1.SUPPLY_SITUATION, 0) = 2 then ''K2''
              when coalesce(t1.SUPPLY_SITUATION, 0) = 3 then ''K4''
              when coalesce(t1.SUPPLY_SITUATION, 0) = 4 then ''K3''
              when coalesce(t1.SUPPLY_SITUATION, 0) = 5 then ''Kaçak''
              else ''Diğer''
            end SUPPLY_SITUATION,
            ------------------------------------------------------------------------    
            nvl(t1.KWH_ACTIVE, 0) + nvl(t1.KWH_SELLER, 0) KWH_ACTIVE,
            ------------------------------------------------------------------------
            nvl(t1.TL_GENERAL_TOTAL, 0) TL_GENERAL_TOTAL
            ------------------------------------------------------------------------
          FROM  
            '||gv_dwh_owner ||'.'||v_src_table_01||' t1, -- I
            '||gv_edw_owner ||'.'||v_src_table_02||' t2, -- D
            '||gv_edw_owner ||'.'||v_src_table_03||' t3, -- A
            '||gv_edw_owner ||'.'||v_src_table_04||' t4, -- T
            '||gv_edw_owner ||'.'||v_src_table_05||' t5, -- C,
            '||gv_edw_owner ||'.'||v_src_table_06||' t6  -- R
          WHERE
            t1.INV_TYPE        = ''TAHAKKUK'' AND
            t1.INSTALLATION_ID = t2.SUBSCRIBER_ID (+) AND
            t1.TARIFF_CODE     = t4.TARIFF_CODE   (+) AND
            t4.TARIFF_TYPE     = t3.ACCOUNT_CODE  (+) AND
            t6.CITY_ID         = t5.CITY_ID       (+) AND
            nvl(t1.I_DISTRICT_CODE,t1.DISTRICT_CODE) = t6.DISTRICT_CODE (+) AND
            nvl(t1.INVOICE_STATUS, 0) not in (2, 9) AND
            t4.TARIFF_TYPE(+) != 23
        ) q1    
      GROUP BY
        q1.COMPANY_CODE,
        q1.INVOICE_DATE,
        q1.SUBSCRIBER_GROUP_NAME,
        q1.CITY_NAME,
        q1.INVOICE_TYPE,
        q1.STATUS_TYPE_ID,
        q1.SUBSCRIBER_KIND,
        q1.SUPPLY_SITUATION  
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;

 ------------------------------------------------------------------------------
  --  Created By     : Ceyhun Kerti
  --  Creation Date  : 2017.04.13
  --  Procedure Name : PRC_TINV0010
  --  Description    : Bankada yapılan tek fatrura tahsilatı
  --
  --  [Modification History]
  --  Created By     : Emrah Tapkan
  --  Created Date   : 2017.05.09
  --  Mükerer Ödemeler Eklendi ve Bankaya Yapılan Tahsilatlar Tiplerine göre 2 ayrıldı.
  --  --------------------------------------------------------------------------
  PROCEDURE PRC_TINV0010
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TINV0010';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'INV_INVOICE_LIVE';
    v_src_table_02  varchar2(30) := 'D_TARIFFS';
    v_src_table_03  varchar2(30) := 'F_BANK_RECURRENT_INVOICE';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_TINV0010';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_dwh_owner,v_table_name);

    gv_dyn_task := '
        CREATE TABLE '|| gv_dwh_owner ||'.'||v_table_name||'
        PARALLEL NOLOGGING COMPRESS AS
      
        SELECT  --+ parallel(I, 16)
            ------------------------------------------------------------------------
            /*Bankada Yapılan Tek Fatrura Tahsilatı*/ ''TFT'' AS TYPE,
            ------------------------------------------------------------------------
            I.COMPANY_CODE,I.CLOSING_DATE,I.INSTALLATION_ID,
            I.IS_GOVERNMENT,I.DISTRICT_CODE,I.TARIFF_CODE,I.STATUS_TYPE_ID,I.CLOSING_CASH_DESK,I.CLOSING_TELLERS,
            I.TL_GENERAL_TOTAL  AS TL_INVOICE_TOTAL,
            NVL(I.TL_DEFAULT_INTEREST,0) + NVL(I.TL_DEFAULT_INTEREST_TAX,0) AS TL_DELAY_TOTAL,
            NVL(I.TL_GENERAL_TOTAL,0) + NVL(I.TL_DEFAULT_INTEREST,0) + NVL(I.TL_DEFAULT_INTEREST_TAX,0) AS TL_GENERAL_TOTAL   
                
        FROM '||gv_dwh_owner ||'.'||v_src_table_01||' I
        WHERE  I.INV_TYPE = ''TAHSILAT''
        AND I.CLOSING_DATE = TO_NUMBER(TO_CHAR(SYSDATE,''yyyymmdd'')) 
        AND I.INVOICE_STATUS NOT IN (2,4,9)
        AND NVL( I.IS_GOVERNMENT,0) = 0

        UNION ALL
        SELECT  --+ parallel(I, 16)
            ------------------------------------------------------------------------
            /*Bankada Yapılan Tek Fatrura Tahsilatı*/ ''BTFT'' AS TYPE,
            ------------------------------------------------------------------------
            I.COMPANY_CODE,I.BANK_COLLECTION_DATE AS CLOSING_DATE,I.INSTALLATION_ID,
            I.IS_GOVERNMENT,I.DISTRICT_CODE,I.TARIFF_CODE,I.STATUS_TYPE_ID,I.CLOSING_CASH_DESK,I.CLOSING_TELLERS,
            I.TL_GENERAL_TOTAL  AS TL_INVOICE_TOTAL,
            NVL(I.TL_DEFAULT_INTEREST,0) + NVL(I.TL_DEFAULT_INTEREST_TAX,0) AS TL_DELAY_TOTAL,
            NVL(I.TL_GENERAL_TOTAL,0) + NVL(I.TL_DEFAULT_INTEREST,0) + NVL(I.TL_DEFAULT_INTEREST_TAX,0) AS TL_GENERAL_TOTAL   
                
        FROM '||gv_dwh_owner ||'.'||v_src_table_01||' I
        WHERE I.INV_TYPE = ''TAHSILAT''
        AND I.BANK_COLLECTION_DATE = TO_NUMBER(TO_CHAR(SYSDATE,''yyyymmdd'')) 
        AND I.INVOICE_STATUS IN (12,13)
        AND I.CLOSING_DATE = 0 
        AND NVL( I.IS_GOVERNMENT,0) = 0

        UNION ALL
        SELECT  --+ parallel(I, 16)
            ------------------------------------------------------------------------
            /*Mükerrer Ödeme Tahsilatları*/ ''MOT'' AS TYPE,
            ------------------------------------------------------------------------
            FI.COMPANY_CODE,I.CLOSING_DATE,I.INSTALLATION_ID,
            I.IS_GOVERNMENT,I.DISTRICT_CODE,FI.TARIFF_CODE, FI.STATUS_TYPE_ID,R_CLOSING_CASH_DESK AS CLOSING_CASH_DESK,I.R_CLOSING_TELLERS AS CLOSING_TELLERS,
            0 AS TL_INVOICE_TOTAL,
            0 AS TL_DELAY_TOTAL,
            I.TL_GENERAL_TOTAL
        FROM  '||gv_edw_owner ||'.'||v_src_table_03||' I 
        LEFT JOIN '||gv_dwh_owner ||'.'||v_src_table_01||' FI ON (I.INVOICE_SERIAL = FI.INVOICE_SERIAL AND I.INVOICE_NUMBER = FI.INVOICE_NUMBER AND  I.INV_TYPE = ''TAHSILAT'')
        LEFT JOIN '||gv_edw_owner ||'.'||v_src_table_02||' T ON (FI.TARIFF_CODE = T.TARIFF_CODE AND T.TARIFF_TYPE = 23)--Değiştirilecek...
        WHERE  I.CLOSING_DATE = TO_NUMBER(TO_CHAR(SYSDATE,''yyyymmdd'')) 
        AND NVL( I.IS_GOVERNMENT,0) = 0
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;


  ------------------------------------------------------------------------------
  --  Created By     : Ceyhun Kerti
  --  Creation Date  : 2017.04.13
  --  Procedure Name : PRC_TINV0020
  --  Description    : 
  --    * Taksit ve Kısmi Tahsilatlar 
  --    * Abonman Tahsilatları
  --
  --  [Modification History]
  --  Created By     : Emrah Tapkan
  --  Created Date   : 2017.05.09
  --  Bankadan yapılan taksitli kısımlar eklendi.
  --  Artı Abonman Taksitleri eklendi.
  --  --------------------------------------------------------------------------
  PROCEDURE PRC_TINV0020
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TINV0020';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'INV_SUBS_INVOICE_LIVE';
    v_src_table_02  varchar2(30) := 'INV_SUBS_INSTALLMENT_LIVE';    
    v_src_table_03  varchar2(30) := 'D_TARIFFS';  
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_TINV0020';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
    
    plib.drop_table(gv_dwh_owner, v_table_name);

    gv_dyn_task := '
      CREATE TABLE '|| gv_dwh_owner ||'.'||v_table_name||'
      PARALLEL NOLOGGING COMPRESS  AS
      
      SELECT  --+ parallel(I, 16)  parallel(SI, 16)
            ------------------------------------------------------------------------
            /*Taksit ve Kısmi Tahsilatlar*/ ''TKT'' AS TYPE,
            ------------------------------------------------------------------------
            I.COMPANY_CODE,I.CLOSING_DATE,I.INSTALLATION_ID,
            I.IS_GOVERNMENT,I.DISTRICT_CODE,SI.TARIFF_CODE,SI.STATUS_TYPE_ID,I.CLOSING_CASH_DESK,I.CLOSING_TELLERS,
            I.TL_TOTAL  AS TL_INVOICE_TOTAL,
            NVL(I.TL_INTERESET_AMOUNT,0) + NVL(I.TL_TOTAL_TAX  ,0) AS TL_DELAY_TOTAL,
            NVL(I.TL_TOTAL,0) + NVL(I.TL_INTERESET_AMOUNT,0) + NVL(I.TL_TOTAL_TAX,0) AS TL_GENERAL_TOTAL  
        FROM '||gv_dwh_owner ||'.'||v_src_table_01||' I,
        LEFT JOIN '||gv_dwh_owner ||'.'||v_src_table_02||' SI ON (I.INSTALLMENT_NO = SI.INSTALLMENT_NO AND I.INSTALLMENT_SERIAL_ID = SI.SERIAL_ID )
        WHERE  I.CLOSING_DATE = to_number(to_char(sysdate,''yyyymmdd''))
        AND I.CANCEL_DATE = 0
        AND I.ITEM_CODE IN (5,50,51,53,54)
        AND NVL( I.IS_GOVERNMENT,0) = 0
        
        UNION ALL
        SELECT  --+ parallel(I, 16)
            ------------------------------------------------------------------------
            /*Bankada Yapılan Taksit ve Kısmi Tahsilatlar*/ ''BTKT''  AS TYPE,
            ------------------------------------------------------------------------
            I.COMPANY_CODE,I.BANK_COLLECTION_DATE AS CLOSING_DATE,I.INSTALLATION_ID,
            I.IS_GOVERNMENT,I.DISTRICT_CODE,I.TARIFF_CODE,I.STATUS_TYPE_ID,I.CLOSING_CASH_DESK,I.CLOSING_TELLERS,
            I.TL_TOTAL  AS TL_INVOICE_TOTAL,
            NVL(I.TL_INTERESET_AMOUNT,0) + NVL(I.TL_TOTAL_TAX,0) +NVL(I.TL_DEFAULT_INTEREST,0) + NVL(TL_DEFAULT_INTEREST_TAX,0) AS TL_DELAY_TOTAL,
            NVL(I.TL_TOTAL,0) +NVL(I.TL_INTERESET_AMOUNT,0)+NVL(I.TL_TOTAL_TAX,0)+NVL(I.TL_DEFAULT_INTEREST,0) + NVL(TL_DEFAULT_INTEREST_TAX,0) AS TL_GENERAL_TOTAL 
        FROM '||gv_dwh_owner ||'.'||v_src_table_01||' I I
        WHERE I.BANK_COLLECTION_DATE = to_number(to_char(sysdate,''yyyymmdd''))
        AND I.INVOICE_STATUS IN (12,13)
        AND I.CLOSING_DATE = 0 
        AND I.CANCEL_DATE = 0
        AND I.ITEM_CODE IN (5,50,51,53,54)  
        AND NVL( I.IS_GOVERNMENT,0) = 0
        
        UNION ALL
        SELECT --+ parallel(I, 16) parallel(SI, 16)
            ------------------------------------------------------------------------
            /*Artı Abonman Fatura*/  ''AAF''AS TYPE,
            ------------------------------------------------------------------------
            I.COMPANY_CODE,I.CLOSING_DATE,I.INSTALLATION_ID,
            SI.IS_GOVERNMENT,I.DISTRICT_CODE,SI.TARIFF_CODE,SI.STATUS_TYPE_ID,I.CLOSING_CASH_DESK,I.CLOSING_TELLERS,
            I.TL_TOTAL  AS TL_INVOICE_TOTAL,
            NVL(I.TL_INTERESET_AMOUNT,0) + NVL(I.TL_TOTAL_TAX  ,0) AS TL_DELAY_TOTAL,
            NVL(I.TL_TOTAL,0) + NVL(I.TL_INTERESET_AMOUNT,0) + NVL(I.TL_TOTAL_TAX,0) AS TL_GENERAL_TOTAL
        FROM  '||gv_dwh_owner ||'.'||v_src_table_01||' I I
        LEFT JOIN '||gv_dwh_owner ||'.'||v_src_table_02||' SI ON (I.INSTALLMENT_NO = SI.INSTALLMENT_NO AND I.INSTALLMENT_SERIAL_ID = SI.SERIAL_ID )
        LEFT JOIN '||gv_edw_owner ||'.'||v_src_table_03||' T ON (SI.TARIFF_CODE = T.TARIFF_CODE AND T.TARIFF_TYPE = 23)
        WHERE  I.CLOSING_DATE = to_number(to_char(sysdate,''yyyymmdd''))
        AND I.ITEM_CODE IN (5,50,51,53,54)
        AND I.CANCEL_DATE != I.CLOSING_DATE
        AND I.CANCEL_DATE > 0
        AND NVL( I.IS_GOVERNMENT,0) = 0
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;


  ------------------------------------------------------------------------------
  --  Created By     : Ceyhun Kerti
  --  Creation Date  : 2017.04.13
  --  Procedure Name : PRC_TINV0030
  --  Description    : Bankada Yapılan Taksit ve Kısmi Tahsilatlar
  --
  --  [Modification History]
  --  Created By     : Emrah Tapkan
  --  Created Date   : 2017.05.09
  --  Artı Fatura İcmal Tashih eklendi.
  --  İptal Tahsilatlar eklendi 2 farklı tipte eklendi.
  --  --------------------------------------------------------------------------
  PROCEDURE PRC_TINV0030
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TINV0030';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'F_INVOICE_SUMMATION_CORRECTION';    
    v_src_table_02  varchar2(30) := 'D_TARIFFS';  
    v_src_table_03  varchar2(30) := 'F_SUBSCRIBER_INVOICE';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_TINV0030';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_dwh_owner, v_table_name);

    gv_dyn_task := '
      CREATE TABLE '|| gv_dwh_owner ||'.'||v_table_name||'
      PARALLEL NOLOGGING COMPRESS
              SELECT --+ parallel(I, 16)  
            ------------------------------------------------------------------------
            /*Artı Fatura İcmal Tashih*/ ''AFIT'' AS TYPE,
            ------------------------------------------------------------------------
            I.COMPANY_CODE,I.CLOSING_DATE,I.INSTALLATION_ID,
            I.IS_GOVERNMENT,I.DISTRICT_CODE,I.TARIFF_CODE,I.STATUS_TYPE_ID,I.CLOSING_CASH_DESK,I.CLOSING_TELLERS,
            I.TL_GENERAL_TOTAL  AS TL_INVOICE_TOTAL,
            NVL(I.TL_DEFAULT_INTEREST,0) + NVL(I.TL_DEFAULT_INTEREST_TAX,0) AS TL_DELAY_TOTAL,
            NVL(I.TL_GENERAL_TOTAL,0) + NVL(I.TL_DEFAULT_INTEREST,0) + NVL(I.TL_DEFAULT_INTEREST_TAX,0) AS TL_GENERAL_TOTAL  
        FROM '||gv_edw_owner ||'.'||v_src_table_01||' I
            LEFT JOIN '||gv_edw_owner ||'.'||v_src_table_02||'  T ON (I.TARIFF_CODE = T.TARIFF_CODE AND TARIFF_TYPE = 23)
        WHERE I.CLOSING_DATE = to_number(to_char(sysdate,''yyyymmdd''))
        AND I.CLOSING_TELLERS NOT IN (9904,9908)-- TAKSİTLENDİRME SONU,TAHSİLAT İPTALİ
        AND I.OPERATION_TYPE = 1 
        AND NVL( I.IS_GOVERNMENT,0) = 0
        AND NOT EXISTS (SELECT NULL FROM '||gv_edw_owner ||'.'||v_src_table_03||' I A WHERE I.INVOICE_SERIAL = A.INVOICE_SERIAL AND I.INVOICE_NUMBER = A.INVOICE_NUMBER)

        UNION ALL
        SELECT --+ parallel(I, 16)  
            ------------------------------------------------------------------------
            /*İptal Tahsilatlar*/ ''IT'' AS TYPE,
            ------------------------------------------------------------------------
            I.COMPANY_CODE,I.CLOSING_DATE,I.INSTALLATION_ID,
            I.IS_GOVERNMENT,I.DISTRICT_CODE,I.TARIFF_CODE,I.STATUS_TYPE_ID,I.CLOSING_CASH_DESK,I.CLOSING_TELLERS,
            I.TL_GENERAL_TOTAL  AS TL_INVOICE_TOTAL,
            NVL(I.TL_DEFAULT_INTEREST,0) + NVL(I.TL_DEFAULT_INTEREST_TAX,0) AS TL_DELAY_TOTAL,
            NVL(I.TL_GENERAL_TOTAL,0) + NVL(I.TL_DEFAULT_INTEREST,0) + NVL(I.TL_DEFAULT_INTEREST_TAX,0) AS TL_GENERAL_TOTAL  
        FROM '||gv_edw_owner ||'.'||v_src_table_01||'  I
        LEFT JOIN '||gv_edw_owner ||'.'||v_src_table_02||'  T ON (I.TARIFF_CODE = T.TARIFF_CODE AND T.TARIFF_TYPE = 23)
        WHERE  I.CLOSING_DATE = to_number(to_char(sysdate,''yyyymmdd''))
        AND I.CLOSING_CASH_DESK = 999
        AND I.CLOSING_TELLERS = 9908
        AND NVL( I.IS_GOVERNMENT,0) = 0
        AND I.OPERATION_TYPE != 11 
        
        UNION ALL
        SELECT --+ parallel(I, 16)   
            ------------------------------------------------------------------------
            /*İptal Tahsilatlar*/ ''IT'' AS TYPE,
            ------------------------------------------------------------------------
            I.COMPANY_CODE,I.CLOSING_DATE,I.INSTALLATION_ID,
            I.IS_GOVERNMENT,I.DISTRICT_CODE,I.TARIFF_CODE,I.STATUS_TYPE_ID,I.CLOSING_CASH_DESK,I.CLOSING_TELLERS,
            I.TL_GENERAL_TOTAL  AS TL_INVOICE_TOTAL,
            NVL(I.TL_DEFAULT_INTEREST,0) + NVL(I.TL_DEFAULT_INTEREST_TAX,0) AS TL_DELAY_TOTAL,
            NVL(I.TL_GENERAL_TOTAL,0) + NVL(I.TL_DEFAULT_INTEREST,0) + NVL(I.TL_DEFAULT_INTEREST_TAX,0) AS TL_GENERAL_TOTAL  
        FROM '||gv_edw_owner ||'.'||v_src_table_01||'  I
        LEFT JOIN '||gv_edw_owner ||'.'||v_src_table_02||'  T ON (I.TARIFF_CODE = T.TARIFF_CODE AND T.TARIFF_TYPE = 23)
        WHERE  I.CLOSING_DATE = to_number(to_char(sysdate,''yyyymmdd''))
        AND I.CLOSING_CASH_DESK = 999
        AND I.CLOSING_TELLERS = 9908
        AND I.OPERATION_TYPE = 11
        AND EXISTS (SELECT NULL FROM '||gv_edw_owner ||'.'||v_src_table_03||'  A WHERE NVL( A.IS_GOVERNMENT,0) = 0 AND  I.INVOICE_SERIAL = A.INVOICE_SERIAL AND I.INVOICE_NUMBER = A.INVOICE_NUMBER) 
    ';
      
    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;


  ------------------------------------------------------------------------------
  --  Created By     : Ceyhun Kerti
  --  Creation Date  : 2017.04.13
  --  Procedure Name : PRC_TINV0040
  --  Description    : Tum Tahsilatlar
  --
  --  [Modification History]
  --  --------------------------------------------------------------------------
  PROCEDURE PRC_TINV0040
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'TINV0040';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'TINV0010';    
    v_src_table_02  varchar2(30) := 'TINV0020';
    v_src_table_03  varchar2(30) := 'TINV0030';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_TINV0040';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);

    plib.drop_table(gv_stg_owner, v_table_name);
    
    gv_dyn_task := '
      CREATE TABLE '|| gv_stg_owner ||'.'||v_table_name||'
      PARALLEL NOLOGGING COMPRESS
      AS
      SELECT --+ parallel(t1, 16)
        t1.TYPE,
        t1.COMPANY_CODE,
        t1.CLOSING_DATE,
        t1.INSTALLATION_ID,
        t1.IS_GOVERNMENT,
        t1.DISTRICT_CODE,
        t1.TARIFF_CODE,
        t1.STATUS_TYPE_ID,
        t1.CLOSING_CASH_DESK,
        t1.CLOSING_TELLERS,
        t1.TL_INVOICE_TOTAL,
        t1.TL_DELAY_TOTAL,
        t1.TL_GENERAL_TOTAL 
      FROM  
        '||gv_stg_owner ||'.'||v_src_table_01||' t1
      UNION ALL
      SELECT --+ parallel(t2, 16)
        t2.TYPE,
        t2.COMPANY_CODE,
        t2.CLOSING_DATE,
        t2.INSTALLATION_ID,
        t2.IS_GOVERNMENT,
        t2.DISTRICT_CODE,
        t2.TARIFF_CODE,
        t2.STATUS_TYPE_ID,
        t2.CLOSING_CASH_DESK,
        t2.CLOSING_TELLERS,
        t2.TL_INVOICE_TOTAL,
        t2.TL_DELAY_TOTAL,
        t2.TL_GENERAL_TOTAL 
      FROM  
        '||gv_stg_owner ||'.'||v_src_table_02||' t2
      UNION ALL
      SELECT --+ parallel(t3, 16)
        t3.TYPE,
        t3.COMPANY_CODE,
        t3.CLOSING_DATE,
        t3.INSTALLATION_ID,
        t3.IS_GOVERNMENT,
        t3.DISTRICT_CODE,
        t3.TARIFF_CODE,
        t3.STATUS_TYPE_ID,
        t3.CLOSING_CASH_DESK,
        t3.CLOSING_TELLERS,
        t3.TL_INVOICE_TOTAL,
        t3.TL_DELAY_TOTAL,
        t3.TL_GENERAL_TOTAL 
      FROM  
        '||gv_stg_owner ||'.'||v_src_table_03||' t3
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;



  ------------------------------------------------------------------------------
  --  Created By     : Ceyhun Kerti
  --  Creation Date  : 2017.04.27
  --  Procedure Name : PRC_INV_COLLECTION_LIVE
  --  Description    : replaces mv collection dashboard
  --
  --  [Modification History]
  --  --------------------------------------------------------------------------
  PROCEDURE PRC_INV_COLLECTION_LIVE
  IS
    ----------------------------------------------------------------------------
    v_table_name    varchar2(30) := 'INV_COLLECTION_LIVE';
    ----------------------------------------------------------------------------
    v_src_table_01  varchar2(30) := 'TINV0040';
    v_src_table_02  varchar2(30) := 'D_SUBSCRIBERS';
    v_src_table_03  varchar2(30) := 'D_TARIFFS';
    v_src_table_04  varchar2(30) := 'D_ACCOUNTING_ACCOUNTS';
    v_src_table_05  varchar2(30) := 'D_REGIONS';
    v_src_table_06  varchar2(30) := 'D_CITIES';
    v_src_table_07  varchar2(30) := 'D_CASH_DESKS';
    v_src_table_08  varchar2(30) := 'D_TELLERS';
    ----------------------------------------------------------------------------
  BEGIN

    gv_proc := 'PRC_INV_COLLECTION_LIVE';
    
    -- Initialize Log Variables
    plib.o_log := 
      log_type.initialize('YES',gv_job_module,gv_job_owner,gv_pck ,gv_proc);
    
    ----------------------------------------------------------------------------
    gv_dyn_task := 'DELETE FROM '||gv_dwh_owner||'.'||v_table_name;
    execute immediate gv_dyn_task;
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    ----------------------------------------------------------------------------  


    gv_dyn_task := '
      INSERT /*+ append nologging */ INTO '|| gv_dwh_owner ||'.'||v_table_name||'
      (
        COLLECTION_DATE,
        TYPE,
        COMPANY_CODE,
        SUBSCRIBER_GROUP_NAME,
        CURRENT_KIND,
        PROVINCE,
        STATUS_TYPE_ID,   
        TL_GENERAL_TOTAL
      )
      SELECT --+ parallel(q1,16)
        q1.COLLECTION_DATE,
        q1.TYPE,
        q1.COMPANY_CODE,
        q1.SUBSCRIBER_GROUP_NAME,
        q1.CURRENT_KIND,
        q1.PROVINCE,
        q1.STATUS_TYPE_ID,
        sum(q1.TL_GENERAL_TOTAL) TL_GENERAL_TOTAL
      FROM
        (
          SELECT --+ parallel(t1, 16) parallel(t2,16)
            t1.TYPE,
            t1.COMPANY_CODE,
            --------------------------------------------------------------------
            to_date(t1.CLOSING_DATE,''yyyymmdd'') COLLECTION_DATE,
            --------------------------------------------------------------------
            case 
              when t1.TARIFF_CODE >0 
              then trim(t4.SUBSCRIBER_GROUP_NAME) 
              else trim(t2.SUBSCRIBER_GROUP_NAME) 
            end SUBSCRIBER_GROUP_NAME,
            --------------------------------------------------------------------
            case
              when t7.BANK_STATUS in (0,1) 
              then t7.BANK_STATUS
              when t7.BANK_STATUS = 2 and t1.CLOSING_CASH_DESK not between 950 and 959 
              then 2
              when t1.CLOSING_CASH_DESK = 999 and t1.CLOSING_TELLERS not in ( 9904, 9908 ) 
              then 3
              when t1.CLOSING_CASH_DESK = 999 and t1.CLOSING_TELLERS = 9908 
              then 4
            end CURRENT_KIND,
            --------------------------------------------------------------------
            trim(t6.CITY_NAME) PROVINCE,
            --------------------------------------------------------------------
            t1.STATUS_TYPE_ID,
            t1.TL_GENERAL_TOTAL
            --------------------------------------------------------------------
          FROM  
            '||gv_dwh_owner ||'.'||v_src_table_01||' t1, -- F
            '||gv_edw_owner ||'.'||v_src_table_02||' t2, -- A
            '||gv_edw_owner ||'.'||v_src_table_03||' t3, -- T 
            '||gv_edw_owner ||'.'||v_src_table_04||' t4, -- H
            '||gv_edw_owner ||'.'||v_src_table_05||' t5, -- B
            '||gv_edw_owner ||'.'||v_src_table_06||' t6, -- I
            '||gv_edw_owner ||'.'||v_src_table_07||' t7, -- V
            '||gv_edw_owner ||'.'||v_src_table_08||' t8  -- VD
          WHERE
            t1.INSTALLATION_ID  = t2.SUBSCRIBER_ID  (+) AND
            t1.TARIFF_CODE      = t3.TARIFF_CODE    (+) AND
            t3.TARIFF_TYPE      = t4.ACCOUNT_CODE   (+) AND
            t1.DISTRICT_CODE    = t5.DISTRICT_CODE  (+) AND
            t5.CITY_ID          = t6.CITY_ID        (+) AND
            t1.CLOSING_CASH_DESK= t7.CASH_DESK_ID   (+) AND
            t1.CLOSING_TELLERS  = t8.TELLERS_CODE   (+)    
        ) q1
      GROUP BY
        q1.TYPE,
        q1.COMPANY_CODE,
        q1.COLLECTION_DATE,
        q1.SUBSCRIBER_GROUP_NAME,
        q1.CURRENT_KIND,
        q1.PROVINCE,
        q1.STATUS_TYPE_ID
    ';

    execute immediate gv_dyn_task;
    
    plib.o_log.log(10,4,NULL,gv_pck||'.'||gv_proc,SQL%ROWCOUNT,gv_dyn_task);
    
    commit;

    EXCEPTION
      WHEN OTHERS THEN
        gv_sql_errc := SQLCODE;
        gv_sql_errm := SQLERRM;
        plib.o_log.log( gv_sql_errc, 1, gv_sql_errm, v_table_name, NULL, gv_dyn_task);
        rollback;
        raise_application_error(gv_sql_errc, gv_sql_errm);  
  END;


END;