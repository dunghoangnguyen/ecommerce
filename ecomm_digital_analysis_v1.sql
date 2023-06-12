use	vn_processing_datamart_temp_db;
--drop table if exists ecom_tb1;
--create table ecom_tb1 as
insert overwrite table ecom_tb1
select	distinct
		mkt.source,
		TO_DATE(mkt.api_import_time) api_import_time,
		tb2.cli_num,
		tb1.pol_num,
		tb1.agt_code,
		NVL(sc.agent_tier,'Unknown') agent_tier,
		tb3.loc_code agent_location_code,
		mkt.batch_trxn_dt,
		ROUND(mode_prem*12/cast(pmt_mode as int),2) batch_trxn_amt,
		--mkt.batch_trxn_amt,
		mkt.shopee_order_id,
		tb1.plan_code_base,
		--tb1.plan_nm,
		tb1.vers_num_base,
		tb1.dist_chnl_cd,
		TO_DATE(tb1.pol_iss_dt) as pol_iss_dt,
		tb1.pol_eff_dt,
		tb1.pol_stat_cd,
		mkt.case_status,
		loc.rh_code agent_code_level_6,
		loc.rh_name agent_name_level_6,
		NVL(mkt.case_count,1) as case_count,
		LAST_DAY(TO_DATE(tb1.pol_iss_dt)) as reporting_date,
		CASE when tb3.loc_code is null then 
					(case when tb1.dist_chnl_cd in ('03','10','14','16','17','18','19','22','23','24','25','29','30','31','32','33','39','41','44','47','49','51','52','53') then ROUND(mkt.batch_trxn_amt*pln.nbv_margin_banca_other_banks,2) -- 'Banca'
						  when tb1.dist_chnl_cd in ('48') then ROUND(mkt.batch_trxn_amt*pln.nbv_margin_other_channel_affinity,2)--'Affinity'
						  when tb1.dist_chnl_cd in ('01', '02', '08', '50', '*') then ROUND(mkt.batch_trxn_amt*pln.nbv_margin_agency,2)--'Agency'
						  when tb1.dist_chnl_cd in ('05','06','07','34','36') then ROUND(mkt.batch_trxn_amt*pln.nbv_margin_dmtm,2)--'DMTM'
						  when tb1.dist_chnl_cd in ('09') then ROUND(mkt.batch_trxn_amt*-1.34,2)--'MI'
					 else ROUND(mkt.batch_trxn_amt*pln.nbv_margin_other_channel,2) END) --'Unknown'
					 when tb1.dist_chnl_cd in ('*') then ROUND(mkt.batch_trxn_amt*pln.nbv_margin_agency,2)--'Agency'
					 when mkt.agent_location_code like 'TCB%' then ROUND(mkt.batch_trxn_amt*pln.nbv_margin_banca_tcb,2) --'TCB'
					 when mkt.agent_location_code like 'SAG%' then ROUND(mkt.batch_trxn_amt*pln.nbv_margin_banca_scb,2) --'SCB'
				else ROUND(mkt.batch_trxn_amt*pln.nbv_margin_other_channel,2) END as NBV --'Unknown'
		from	vn_published_cas_db.tpolicys tb1
			inner join
				vn_published_cas_db.tclient_policy_links tb2
			 on	tb1.pol_num=tb2.pol_num and	tb2.link_typ='O'
		 	left join
				vn_published_reports_db.tmkt_submission mkt			 
			 on	tb1.pol_num=mkt.pol_num
			left join
				vn_published_campaign_db.nbv_margin_histories pln 
			 on pln.plan_code=tb1.plan_code_base and 
			CONCAT((case when FLOOR(month(tb1.pol_iss_dt)/3.1)=0 then -1
					 when FLOOR(month(tb1.pol_iss_dt)/3.1)=1 then -1
					 else 0 end) + year(tb1.pol_iss_dt), ' Q',
			   	   (case when FLOOR(month(tb1.pol_iss_dt)/3.1)=0 then 3
					 when FLOOR(month(tb1.pol_iss_dt)/3.1)=1 then 4
					 when FLOOR(month(tb1.pol_iss_dt)/3.1)=2 then 1
					 else 2 end))=pln.effective_qtr
			left join
				vn_published_analytics_db.agent_scorecard sc
			 on tb1.agt_code=sc.agt_code and LAST_DAY(tb1.pol_iss_dt)=sc.monthend_dt
	left join
		vn_published_ams_db.tams_agents tb3
	 on	tb1.wa_cd_1=tb3.agt_code			 
	left join
		vn_published_reports_db.loc_to_sm_mapping loc
	 on tb3.loc_code=loc.loc_cd
where	YEAR(TO_DATE(tb1.pol_iss_dt)) >= YEAR(CURRENT_DATE)-1
	and	SUBSTR(tb1.plan_code_base,1,3) in ('FDB','BIC','PN0')
;

--drop table if exists ecom_cat;
--create table ecom_cat as
insert overwrite table ecom_cat
select	tb1.cli_num, 'Y' agt_ind
		 from	vn_published_datamart_db.tcustdm_daily tb1
		 	inner join
		 		vn_published_datamart_db.tagtdm_daily tb2
		 	on	tb1.id_num=tb2.id_num
;

--drop table if exists ecom_org;
--create table ecom_org as
insert overwrite table ecom_org
select	 po_num, MIN(frst_iss_dt) frst_iss_dt
from	vn_published_datamart_db.tpolidm_daily
group by po_num
;

--drop table if exists ecom_hld;
--create table ecom_hld as
insert overwrite table ecom_hld
select	po_num, COUNT(DISTINCT pol_num) no_pols, SUM(tot_ape) tot_ape
from	vn_published_datamart_db.tpolidm_daily
where	pol_stat_cd in ('1','2','3','5')
group by
		po_num
;

--drop table if exists ecom_upsell;
--create table ecom_upsell as
insert overwrite table ecom_upsell
select	tb2.cli_num,
		tb1.pol_num,
		tb1.plan_code_base,
		pln.nbv_factor_group,
		TO_DATE(tb1.pol_iss_dt) as pol_iss_dt,
		tb1.agt_code,
		--tb1.face_amt,
		CASE when tb1.pmt_mode = '12' then 1 * tb1.mode_prem * 1000 
		  when tb1.pmt_mode = '06' then 2 * tb1.mode_prem * 1000 
		  when tb1.pmt_mode = '03' then 4 * tb1.mode_prem * 1000 
		  else 12 * tb1.mode_prem * 1000 
		END as APE_Upsell,
		CASE when agt.loc_code is null then 
				(case when tb1.dist_chnl_cd in ('03','10','14','16','17','18','19','22','23','24','25','29','30','31','32','33','39','41','44','47','49') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*nbv.nbv_margin_banca_other_banks,2) -- 'Banca'
					  when tb1.dist_chnl_cd in ('48') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*nbv.nbv_margin_other_channel_affinity,2)--'Affinity'
					  when tb1.dist_chnl_cd in ('01', '02', '08', '50', '*') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*nbv.nbv_margin_agency,2)--'Agency'
					  when tb1.dist_chnl_cd in ('05','06','07','34','36') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*nbv.nbv_margin_dmtm,2)--'DMTM'
					  when tb1.dist_chnl_cd in ('09') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*-1.34041044648343,2)--'MI'
				 else ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*nbv.nbv_margin_other_channel,2) END) --'Unknown'
				 when tb1.dist_chnl_cd in ('*') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*nbv.nbv_margin_agency,2)--'Agency'
				 when agt.loc_code like 'TCB%' then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*nbv.nbv_margin_banca_tcb,2) --'TCB'
				 when agt.loc_code like 'SAG%' then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*nbv.nbv_margin_banca_scb,2) --'SCB'
			else ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*nbv.nbv_margin_other_channel,2) END as NBV_Upsell		
 from  vn_published_cas_db.tpolicys tb1
	inner join 
		vn_published_cas_db.tclient_policy_links tb2 
	 on tb1.pol_num=tb2.pol_num
	left join
		vn_published_ams_db.tams_agents agt
	 on	tb1.wa_cd_1=agt.agt_code
	left join
		vn_published_campaign_db.nbv_margin_histories nbv on nbv.plan_code=tb1.plan_code_base and 
		CONCAT((case when FLOOR(month(tb1.pol_iss_dt)/3.1)=0 then -1
					 when FLOOR(month(tb1.pol_iss_dt)/3.1)=1 then -1
					 else 0 end) + year(tb1.pol_iss_dt), ' Q',
			   	   (case when FLOOR(month(tb1.pol_iss_dt)/3.1)=0 then 3
					 when FLOOR(month(tb1.pol_iss_dt)/3.1)=1 then 4
					 when FLOOR(month(tb1.pol_iss_dt)/3.1)=2 then 1
					 else 2 end))=nbv.effective_qtr
	left join
	    vn_published_campaign_db.vn_plan_code_map pln on tb1.plan_code_base=pln.plan_code
 where  tb1.plan_code_base not in ('FDB01','BIC01','BIC02','BIC03','BIC04','PN001')
	and tb2.link_typ = 'O'
	and tb1.pol_stat_cd not in ('8','A','N','R','X')
;

--drop table if exists vn_processing_datamart_temp_db.ecomm_digital_analysis;
--create table vn_processing_datamart_temp_db.ecomm_digital_analysis as
insert overwrite table ecomm_digital_analysis
select	ecom.source as source,
		ecom.api_import_time as api_import_time,
		ecom.cli_num as cli_num,
		cus.sex_code as gender,
		cus.cur_age as age_curr,
		CASE
        WHEN hld.tot_ape >= 20000
            AND hld.tot_ape < 65000
            AND cus.cur_age-cus.frst_iss_age >= 10
										THEN '4.Silver'
        WHEN hld.tot_ape >= 65000
            AND hld.tot_ape < 150000    THEN '3.Gold'
        WHEN hld.tot_ape >= 150000
            AND hld.tot_ape < 300000    THEN '2.Platinum'
        WHEN hld.tot_ape >= 300000      THEN '1.Platinum Elite'
										ELSE '5.Not VIP'
		END as VIP_cat,
		cus.city as city,
		cus.cur_age-cus.frst_iss_age as tenure,
		hld.tot_ape as tot_ape,
		NVL(cat.agt_ind,'N') as agent_ind,
		ecom.pol_num as pol_num,
		ecom.agt_code as agent_code,
		ecom.agent_tier as agent_tier,
		ecom.agent_location_code as agent_location_code,
		ecom.batch_trxn_dt as batch_trxn_dt,
		ecom.batch_trxn_amt as batch_trxn_amt,
		ecom.nbv as NBV,
		ecom.shopee_order_id as shopee_order_id,
		ecom.plan_code_base as plan_code,
		--ecom.plan_nm as plan_name,
		ecom.vers_num_base as vers_num,
		ecom.pol_iss_dt as pol_iss_dt,
		ecom.pol_eff_dt as policy_effective_date,
		ecom.pol_stat_cd as policy_status_code,
		ecom.case_status as case_status,
		ecom.agent_code_level_6 as agent_code_level_6,
		ecom.agent_name_level_6 as agent_name_level_6,
		ecom.case_count as case_count,
		ecom.reporting_date as reporting_date, 
		hld.no_pols as no_pols,
        noe.pol_num as CC_Upsell, 
		noe.plan_code_base as CC_Plancode,
		noe.nbv_factor_group as CC_ProductGroup,
        TO_DATE(noe.pol_iss_dt) as iss_dt_Upsell, 
        NVL(noe.APE_Upsell,0) as APE_Upsell,
		CASE when org.po_num is null then 'Y' else 'N' END as new_ind,
		NVL(noe.NBV_Upsell,0) as NBV_Upsell
from	ecom_tb1 ecom
inner join
		vn_published_datamart_db.tcustdm_daily cus
	on	ecom.cli_num=cus.cli_num
left join
		ecom_cat cat
	on ecom.cli_num=cat.cli_num
left join
		ecom_org org
	 on	ecom.cli_num=org.po_num AND ecom.pol_iss_dt>org.frst_iss_dt
left join
		ecom_hld hld
	 on	ecom.cli_num=hld.po_num
left join 
		ecom_upsell noe
	 on	ecom.cli_num=noe.cli_num and ecom.pol_iss_dt<noe.pol_iss_dt and DATEDIFF(noe.pol_iss_dt,ecom.pol_iss_dt)<=180
;