
procedure ws_consult_payment(
  p_login         in varchar2 ,
  p_source_type   in  mw_SOURCE_TYPE.source_type%Type,
  p_payment_id    in varchar2,
  pi_id           in varchar2,
  pi_idCreditor   in varchar2,
  pi_isPortal     in integer,
  p_code_out      out integer  ,
  p_value_out     out SYS_REFCURSOR  ,
  p_error_out     OUT SYS_REFCURSOR
  ) as

----------------------------------------------------------------------
-- - Name          : ws_consult_payment
-- - Author        : Marc DELBOS  (CSC)
-- - Creation date : 25-AUG-2011
-- - Version       : 1.0

-- - Description   :

--   search payment into the table mw_paymennt  with payment_id
--   return 0 into p_code_out when ok ,
--   return  1 when error fonctionnelle
--   return  2 when warning non bloquante
--   return -1 when warning non bloquante
--   return the value into p_value_out when ok
--   return the error into p_error_out when error or warning

--   STATUT        : en cours

-- Maintenance History:
--
-- Date         Name  Version  Remarks
-- -----------  ----  -------  -----------------------------------
-- 25/08/2011   MDE   1.0      Initial version
----------------------------------------------------------------------


  lr_error                P_mw_mandate_error_log.REC_ERROR;
  ln_USER_ACTION_LOG_ID   mw_user_actions_log.USER_ACTION_LOG_ID%type;
  l_mandate_id            mw_mandate.mandate_id%type;
  ln_count                number;
  ln_count_schedule       number;
  ln_schedule             integer:=0;
  l_DOCUMENT_REFERENCE    mw_documents.DOCUMENT_REFERENCE%type;
  lv_unit_id_by_user   varchar2(5000)  := null;
  lv_req_c             varchar2(22000) := 'select count(*) nbre_enreg ';
  lv_req_d             varchar2(22000) := 'select * ';
  lv_req               varchar2(10000) := 'select m.*, 1 as line_count, DECODE(SIGN(nvl(m.PRE_NOTIFICATION_REQ_DATE,sysdate)-sysdate), 1, 1, 0) as PRE_NOTIF, ';
  l_role_id            mw_user.role_id%type;
  l_count_user_login   number;
  l_count_rtransaction number;
  ln_rtransaction     integer:=0;
  l_USER_LOGIN         mw_mandate__user.user_login%type;
  v_login              varchar2(100) ;
  
  l_payment_status     mw_payment.STATUS%type;
  l_bic_change 		boolean:=false;
  l_iban_change 	boolean:=false;
  l_count_audit     number:=0;
  l_bic             varchar2(100) ;
  l_iban			varchar2(100) ;
  l_date_emise	    mw_payment.collection_date%type :=sysdate;	

  BEGIN

  if pi_isPortal = 1 then
      select mw_user.login into v_login from mw_user
                                        where mw_user.id = pi_id
                                        and mw_user.id_creditor = pi_idCreditor ;
   else
      v_login := p_login ;
   end if ;

  P_mw_globals.sp_dbms('ws_consult_payment payment_id : ' || p_payment_id);

    P_MW_MANDATE_ERROR_LOG.log_user_action_log
       ('GENERE',
        ln_USER_ACTION_LOG_ID,
        null,
        null,
        null,
        null);

  P_MW_MANDATE_ERROR_LOG.log_user_action_log
       ('INSERT',
        ln_USER_ACTION_LOG_ID,
        p_login,
        2,  -- consultation
        localtimestamp,
        null);

   lv_unit_id_by_user := p_mw_unit.search_unit_id_by_login(ln_USER_ACTION_LOG_ID,v_login);
   if lv_unit_id_by_user is not null then
       select mandate_id into l_mandate_id from mw_payment where payment_id = p_payment_id;
	   
	   select status into l_payment_status from mw_payment where payment_id = p_payment_id;
	   
	   
	   if l_payment_status = 'PICCL' or l_payment_status ='PISNT'  or l_payment_status ='PITCCL' then
             
           
          select collection_date into  l_date_emise from mw_payment where payment_id = p_payment_id and collection_date is not null; 
         
          select count(*) into l_count_audit from MW_AUDIT where  FIELD_UPDATED='DBTR_BIC'  AND ACTION=3 and mandate_id=l_mandate_id AND ACTION_DATE > l_date_emise;
          if l_count_audit > 0 then
            l_bic_change := true;
             
          else 
            select count(*) into l_count_audit from MW_AUDIT where  FIELD_UPDATED='DBTR_IBAN'  AND ACTION=3 and mandate_id=l_mandate_id AND ACTION_DATE > l_date_emise;	
            if l_count_audit > 0 then
              l_iban_change := true;
              
            end if;
          end if;
          if l_bic_change then 
          
            select previous_value into l_bic from(
			select previous_value  from mw_audit where FIELD_UPDATED='DBTR_BIC'  AND ACTION=3 and mandate_id=l_mandate_id and action_date>l_date_emise order by action_date 
           ) where rownum=1;
		   select previous_value into l_iban from(
			select previous_value  from mw_audit where FIELD_UPDATED='DBTR_IBAN'  AND ACTION=3 and mandate_id=l_mandate_id and action_date>l_date_emise order by action_date 
           ) where rownum=1;
          elsif l_iban_change then
         
              select m.dbtr_bic into l_bic from mw_mandate m where m.mandate_id=l_mandate_id;
            select previous_value into l_iban from(
             select previous_value  from mw_audit where FIELD_UPDATED='DBTR_IBAN'  AND ACTION=3 and mandate_id=l_mandate_id and action_date>l_date_emise order by action_date 
           ) where rownum=1;
              else
           
              select m.dbtr_bic into l_bic from mw_mandate m where m.mandate_id=l_mandate_id;
              select m.dbtr_iban into l_iban from mw_mandate m where m.mandate_id=l_mandate_id;
          
          end if;
           
        if l_mandate_id is not null then
                  open p_value_out FOR
             select p.payment_id, p.collection_id, p.mandate_ref, p.contract_id, p.cdtr_uci,
                    p.collection_due_date, p.is_final, p.amount, p.status, p.pain_id, p.mandate_id, p.file_id, p.collection_label,
                    p.collection_type, p.cdtr_bic, p.cdtr_iban, p.unit_id, p.source_type, p.id_source, p.end_to_end, p.payment_type, p.source_format,
                    p.collection_date, p.line_number, p.lablat_type, p.lablat_answer
                    , m.cdtr_nm, m.dbtr_nm as dbtr_nm, l_bic as dbtr_bic, l_iban as dbtr_iban, m.transaction_type_id, 1 as line_count from mw_payment p, mw_mandate m
                    where payment_id = p_payment_id
                    and p.mandate_id = m.mandate_id
                    and p.payment_type in ('SDD', 'SEPA_PAYMENT')
                    union select p.payment_id, p.collection_id, p.mandate_ref, p.contract_id, p.cdtr_uci,
                    p.collection_due_date, p.is_final, p.amount, p.status, p.pain_id, p.mandate_id, p.file_id, p.collection_label,
                    p.collection_type, p.cdtr_bic, p.cdtr_iban, p.unit_id, p.source_type, p.id_source, p.end_to_end, p.payment_type, p.source_format,
                    p.collection_date, p.line_number, p.lablat_type, p.lablat_answer
                    ,p.cdtr_nm, p.dbtr_nm as dbtr_nm, l_bic as dbtr_bic, l_iban as dbtr_iban, null as transaction_type_id, 1 as line_count from mw_payment p
                    where payment_id = p_payment_id
                    and p.payment_type = 'SCT';
                    
            else
              open p_value_out FOR
             select p.payment_id, p.collection_id, p.mandate_ref, p.contract_id, p.cdtr_uci,
                    p.collection_due_date, p.is_final, p.amount, p.status, p.pain_id, p.mandate_id, p.file_id, p.collection_label,
                    p.collection_type, p.cdtr_bic, p.cdtr_iban, p.unit_id, p.source_type, p.id_source, p.end_to_end, p.payment_type, p.source_format,
                    p.collection_date, p.line_number
                    ,p.cdtr_nm, p.dbtr_nm as dbtr_nm, l_bic as dbtr_bic, l_iban as dbtr_iban, null as transaction_type_id, 1 as line_count from mw_payment p
                    where payment_id = p_payment_id
                    and p.payment_type in ('SDD', 'SEPA_PAYMENT', 'SCT');
            end if;	
		
	  else
	   
        if l_mandate_id is not null then
              open p_value_out FOR
                select p.payment_id, p.collection_id, p.mandate_ref, p.contract_id, p.cdtr_uci,
                p.collection_due_date, p.is_final, p.amount, p.status, p.pain_id, p.mandate_id, p.file_id, p.collection_label,
                p.collection_type, p.cdtr_bic, p.cdtr_iban, p.unit_id, p.source_type, p.id_source, p.end_to_end, p.payment_type, p.source_format,
                p.collection_date, p.line_number, p.lablat_type, p.lablat_answer
                , m.cdtr_nm, m.dbtr_nm as dbtr_nm, m.dbtr_bic as dbtr_bic, m.dbtr_iban as dbtr_iban, m.transaction_type_id, 1 as line_count from mw_payment p, mw_mandate m
                where payment_id = p_payment_id
                and p.mandate_id = m.mandate_id
                and p.payment_type in ('SDD', 'SEPA_PAYMENT')
                union select p.payment_id, p.collection_id, p.mandate_ref, p.contract_id, p.cdtr_uci,
                p.collection_due_date, p.is_final, p.amount, p.status, p.pain_id, p.mandate_id, p.file_id, p.collection_label,
                p.collection_type, p.cdtr_bic, p.cdtr_iban, p.unit_id, p.source_type, p.id_source, p.end_to_end, p.payment_type, p.source_format,
                p.collection_date, p.line_number, p.lablat_type, p.lablat_answer
                ,p.cdtr_nm, p.dbtr_nm as dbtr_nm, p.dbtr_bic as dbtr_bic, p.dbtr_iban as dbtr_iban, null as transaction_type_id, 1 as line_count from mw_payment p
                where payment_id = p_payment_id
                and p.payment_type = 'SCT';
        else
          open p_value_out FOR
          select p.payment_id, p.collection_id, p.mandate_ref, p.contract_id, p.cdtr_uci,
                p.collection_due_date, p.is_final, p.amount, p.status, p.pain_id, p.mandate_id, p.file_id, p.collection_label,
                p.collection_type, p.cdtr_bic, p.cdtr_iban, p.unit_id, p.source_type, p.id_source, p.end_to_end, p.payment_type, p.source_format,
                p.collection_date, p.line_number
                ,p.cdtr_nm, p.dbtr_nm as dbtr_nm, p.dbtr_bic as dbtr_bic, p.dbtr_iban as dbtr_iban, null as transaction_type_id, 1 as line_count from mw_payment p
                where payment_id = p_payment_id
                and p.payment_type in ('SDD', 'SEPA_PAYMENT', 'SCT');
        end if;
	end if;
  else
      open p_value_out FOR
        select p.*, 0 as line_count from mw_payment p
        where 1=0;
   end if;
  p_code_out := 0;

EXCEPTION
  WHEN others THEN
    P_mw_MANDATE_ERROR_LOG.log_error
    (null                     ,                       --lr_error.g_ERROR_ID
    'MSG_000'              ,                       --lr_error.g_ERROR_CODE_ID
     ln_USER_ACTION_LOG_ID    ,                       --lr_error.g_USER_ACTION_LOG_ID
     to_char(SQLERRM)         ,                       --lr_error.g_ERROR_VALUE
     null                     ,                       --lr_error.g_ERROR_LINE_NUMBER
     null                     ,                       --lr_error.g_ERROR_LINE
    'consult_mandate : '||to_char(SQLERRM),           --lr_error.g_ERROR_DESCRIPTION
     localtimestamp);                                        --lr_error.g_DATE_CREATE

     p_code_out := -1;
     open   p_error_out FOR
     select * from mw_error
     where  USER_ACTION_LOG_ID   =  ln_USER_ACTION_LOG_ID;

END ws_consult_payment;