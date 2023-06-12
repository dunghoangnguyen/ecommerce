select	ecom.source as source,
		ecom.api_import_time as api_import_time,
		ecom.cli_num as cli_num,
		ecom.pol_num as pol_num,
		ecom.agt_code as agent_code,
		ecom.agent_tier as agent_tier,
		ecom.agent_location_code as agent_location_code,
		ecom.batch_trxn_dt as batch_trxn_dt,
		ecom.batch_trxn_amt as batch_trxn_amt,
		ecom.shopee_order_id as shopee_order_id,
		ecom.plan_code_base as plan_code,
		--ecom.plan_nm as plan_name,
		ecom.vers_num_base as vers_num,
		ecom.pol_iss_dt as pol_iss_dt,
		ecom.pol_eff_dt as policy_effective_date,
		ecom.pol_stat_cd as policy_status_code,
		ecom.case_status as case_status,
		ecom.agent_code_level_6 as agent_code_level_6,
		ecom.case_count as case_count,
		ecom.reporting_date as reporting_date, 
        noe.pol_num as CC_Upsell, 
        TO_DATE(noe.pol_iss_dt) as iss_dt_Upsell, 
        NVL(noe.APE_Upsell,0) as APE_Upsell,
		CASE when org.po_num is null then 'Y' else 'N' END as new_ind,
		ecom.NBV as NBV,
		NVL(noe.NBV_Upsell,0) as NBV_Upsell
from	(select	distinct
				mkt.source,
				TO_DATE(mkt.api_import_time) api_import_time,
				tb2.cli_num,
				tb1.pol_num,
				tb1.agt_code,
				NVL(sc.agent_tier,'Unknown') agent_tier,
		 		mkt.agent_location_code,
				mkt.batch_trxn_dt,
                ROUND(mode_prem*12/cast(pmt_mode as int),0) batch_trxn_amt,
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
				mkt.agent_code_level_6,
				NVL(mkt.case_count,1) as case_count,
		 		CASE when mkt.agent_location_code is null then 
					(case when tb1.dist_chnl_cd in ('03','10','14','16','17','18','19','22','23','24','25','29','30','31','32','33','39','41','44','47','49','51','52','53') then ROUND((mode_prem*12/cast(pmt_mode as int))*NVL(pln.nbv_margin_banca_other_banks,nbv.nbv_margin_banca_other_banks),2) -- 'Banca'
						  when tb1.dist_chnl_cd in ('48') then ROUND((mode_prem*12/cast(pmt_mode as int))*NVL(pln.nbv_margin_other_channel_affinity,nbv.nbv_margin_other_channel_affinity),2)--'Affinity'
						  when tb1.dist_chnl_cd in ('01', '02', '08', '50', '*') then ROUND((mode_prem*12/cast(pmt_mode as int))*NVL(pln.nbv_margin_agency,nbv.nbv_margin_agency),2)--'Agency'
						  when tb1.dist_chnl_cd in ('05','06','07','34','36') then ROUND((mode_prem*12/cast(pmt_mode as int))*NVL(pln.nbv_margin_dmtm,nbv.nbv_margin_dmtm),2)--'DMTM'
						  when tb1.dist_chnl_cd in ('09') then ROUND((mode_prem*12/cast(pmt_mode as int))*-1.34,2)--'MI'
					 else ROUND((mode_prem*12/cast(pmt_mode as int))*NVL(pln.nbv_margin_other_channel,nbv.nbv_margin_other_channel),2) END) --'Unknown'
					 when tb1.dist_chnl_cd in ('01', '02', '08', '50', '*') then ROUND((mode_prem*12/cast(pmt_mode as int))*NVL(pln.nbv_margin_agency,nbv.nbv_margin_agency),2)--'Agency'
					 when mkt.agent_location_code like 'TCB%' then ROUND((mode_prem*12/cast(pmt_mode as int))*NVL(pln.nbv_margin_banca_tcb,nbv.nbv_margin_banca_tcb),2) --'TCB'
					 when mkt.agent_location_code like 'SAG%' then ROUND((mode_prem*12/cast(pmt_mode as int))*NVL(pln.nbv_margin_banca_scb,nbv.nbv_margin_banca_scb),2) --'SCB'
				else ROUND((mode_prem*12/cast(pmt_mode as int))*NVL(pln.nbv_margin_other_channel,nbv.nbv_margin_other_channel),2) END as NBV, --'Unknown'
				mkt.reporting_date
		from	vn_published_cas_db.tpolicys tb1
			inner join
				vn_published_cas_db.tclient_policy_links tb2
			 on	tb1.pol_num=tb2.pol_num and	tb2.link_typ='O' and tb2.rec_status='A'
		 	left join
				vn_published_reports_db.tmkt_submission mkt			 
			 on	tb1.pol_num=mkt.pol_num
			left join
				vn_published_campaign_db.nbv_margin_histories pln 
			 on pln.plan_code=tb1.plan_code_base and 
			    floor(months_between(tb1.pol_iss_dt,pln.effective_date)) between 0 and 2
			left join
				vn_published_campaign_db.vn_plan_code_map nbv
			 on	nbv.plan_code=tb1.plan_code_base
			left join
				vn_published_analytics_db.agent_scorecard sc
			 on tb1.agt_code=sc.agt_code and LAST_DAY(tb1.pol_iss_dt)=sc.monthend_dt
		where	YEAR(TO_DATE(tb1.pol_iss_dt)) >= YEAR(CURRENT_DATE)-2
			and	tb1.plan_code_base in ('FDB01','BIC01','BIC02','BIC03','BIC04','PN001')
			and tb1.pol_stat_cd not in ('8','A','N','R','X')
		) ecom
left join
		(select	po_num, MIN(frst_iss_dt) frst_iss_dt
		 from	vn_published_datamart_db.tpolidm_daily
		 group by po_num
		) org
	 on	ecom.cli_num=org.po_num AND ecom.pol_iss_dt>org.frst_iss_dt		
left join 
		(select	tb2.cli_num,
				tb1.pol_num,
				TO_DATE(tb1.pol_iss_dt) as pol_iss_dt,
				tb1.agt_code,
				CASE when tb1.pmt_mode = '12' then 1 * tb1.mode_prem * 1000 
				  when tb1.pmt_mode = '06' then 2 * tb1.mode_prem * 1000 
				  when tb1.pmt_mode = '03' then 4 * tb1.mode_prem * 1000 
				  else 12 * tb1.mode_prem * 1000 
				END as APE_Upsell,
				CASE when agt.loc_code is null then 
						(case when tb1.dist_chnl_cd in ('03','10','14','16','17','18','19','22','23','24','25','29','30','31','32','33','39','41','44','47','49','51','52','53') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_banca_other_banks,nbv.nbv_margin_banca_other_banks),2) -- 'Banca'
							  when tb1.dist_chnl_cd in ('48') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_other_channel_affinity,nbv.nbv_margin_other_channel_affinity),2)--'Affinity'
							  when tb1.dist_chnl_cd in ('01', '02', '08', '50', '*') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_agency,nbv.nbv_margin_agency),2)--'Agency'
							  when tb1.dist_chnl_cd in ('05','06','07','34','36') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_dmtm,nbv.nbv_margin_dmtm),2)--'DMTM'
							  when tb1.dist_chnl_cd in ('09') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*-1.34,2)--'MI'
						 else ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_other_channel,nbv.nbv_margin_other_channel),2) END) --'Unknown'
						 when tb1.dist_chnl_cd in ('01', '02', '08', '50', '*') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_agency,nbv.nbv_margin_agency),2)--'Agency'
						 when agt.loc_code like 'TCB%' then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_banca_tcb,nbv.nbv_margin_banca_tcb),2) --'TCB'
						 when agt.loc_code like 'SAG%' then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_banca_scb,nbv.nbv_margin_banca_scb),2) --'SCB'
				    else ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_other_channel,nbv.nbv_margin_other_channel),2) END as NBV_Upsell		
		 from  vn_published_cas_db.tpolicys tb1
			inner join 
				vn_published_cas_db.tclient_policy_links tb2 
			 on tb1.pol_num=tb2.pol_num
			left join
				vn_published_ams_db.tams_agents agt
			 on	tb1.wa_cd_1=agt.agt_code
			left join
				vn_published_campaign_db.nbv_margin_histories pln 
			 on pln.plan_code=tb1.plan_code_base and 
			    floor(months_between(tb1.pol_iss_dt,pln.effective_date)) between 0 and 2
			left join
				vn_published_campaign_db.vn_plan_code_map nbv
			 on nbv.plan_code=tb1.plan_code_base
		 where  tb1.plan_code_base not in ('FDB01','BIC01','BIC02','BIC03','BIC04','PN001')
			and tb2.link_typ = 'O'
			and tb2.rec_status='A'
			and tb1.pol_stat_cd not in ('8','A','N','R','X')
		) noe
	 on	ecom.cli_num=noe.cli_num and ecom.pol_iss_dt<noe.pol_iss_dt and datediff(noe.pol_iss_dt,ecom.pol_iss_dt)<=180
