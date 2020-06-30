/*
Bring all existing code lists together to create one text file to submit 
alongside the ISAC

aurum_codelist_af
aurum_codelist_cva
aurum_codelist_mi
aurum_codelist_pad
aurum_codelist_tia
aurum_codelist_ua
aurum_codelist_vte
aurum_codelist_hf
aurum_codelist_ihd

codelist_SMI_cprd_aurum
depression_codelist_Aurum_v3
cr_codelist_anxiety
selfharm_codelist_Aurum_v1
ED_codes
OCD


codelistAurum_respiratory_aeasthma
codelistAurum_respiratory_aecopd
codelistAurum_respiratory_asthmadenominator
codelistAurum_respiratory_copddenominator


diab_emergency_codes_rm_aurum
diab_emergency_codes_rm_icd10


Codelist_ethnicity_aurum_sml

*/


global allcodes "/Users/katemansfield/Dropbox/LSHTM/2020/Projects/14-covid-collateral/codelists/202006_codeBrowsers/CPRDAurumMedical"


cd "/Users/katemansfield/Dropbox/LSHTM/2020/Projects/14-covid-collateral/codelists/fromJ"

/*******************************************************************************
#1. go through each dataset check that the appropriate information is in there
*******************************************************************************/
* AF
use "aurum_codelist_af", clear
describe
drop term
capture confirm numeric variable medcodeid
duplicates drop

merge 1:1 medcodeid using $allcodes, keep(match master) nogen
drop release
gen list_af=1
compress
save colat-aurum-af, replace



* CVA
use aurum_codelist_cva, clear
describe
drop term
capture confirm numeric variable medcodeid
duplicates drop

merge 1:1 medcodeid using $allcodes, keep(match master) nogen
drop release
gen list_cva=1
compress
save colat-aurum-cva, replace


* MI
use aurum_codelist_mi, clear
describe
drop term
capture confirm numeric variable medcodeid
duplicates drop

merge 1:1 medcodeid using $allcodes, keep(match master) nogen
drop release
gen list_mi=1
compress
save colat-aurum-mi, replace


* pad
use aurum_codelist_pad, clear
describe
drop term
capture confirm numeric variable medcodeid
duplicates drop

merge 1:1 medcodeid using $allcodes, keep(match master) nogen
drop release
gen list_pad=1
compress
save colat-aurum-pad, replace


* tia
use aurum_codelist_tia, clear
describe
drop term
capture confirm numeric variable medcodeid
duplicates drop

merge 1:1 medcodeid using $allcodes, keep(match master) nogen
drop release
gen list_tia=1
compress
save colat-aurum-tia, replace











/*******************************************************************************
#1. go through each dataset and make the medcodeid variable a string
*******************************************************************************/
* see if I can merge in the other codelists




local lists " `lists' "aurum_codelist_ua" "
local lists " `lists' "aurum_codelist_vte" "
local lists " `lists' "aurum_codelist_hf" "
local lists " `lists' "aurum_codelist_ihd" "
local lists " `lists' "codelist_SMI_cprd_aurum" "
local lists " `lists' "depression_codelist_Aurum_v3" "
local lists " `lists' "cr_codelist_anxiety" "
local lists " `lists' "selfharm_codelist_Aurum_v1" "
local lists " `lists' "ED_codes" "
local lists " `lists' "OCD" "
local lists " `lists' "codelistAurum_respiratory_aeasthma" "
local lists " `lists' "codelistAurum_respiratory_aecopd" "
local lists " `lists' "codelistAurum_respiratory_asthmadenominator" "
local lists " `lists' "codelistAurum_respiratory_copddenominator" "
local lists " `lists' "diab_emergency_codes_rm_aurum" "
local lists " `lists' "Codelist_ethnicity_aurum_sml" "


foreach x in `lists' {
	use `x', clear
	capture confirm numeric variable medcodeid
	if !_rc {
		drop if medcodeid==.
		tostring medcodeid, force replace
		duplicates drop medcodeid, force
		if "`x'"=="depression_codelist_Aurum_v3" {
			rename marker cat
		} /*end if "`x'"=="depression_codelist_Aurum_v3"...*/
    } /*end if !_rc*/
	capture confirm numeric variable snomedctconceptid
	if !_rc {
		tostring snomedctconceptid, force replace
	} /*end if !_rc*/
	capture confirm numeric variable snomedctdescriptionid
	if !_rc {
		tostring snomedctdescriptionid, force replace
	} /*end if !_rc*/	
	save "`x'_new", replace
} /*end foreach...*/




	








/*******************************************************************************
#2. open define code list and merge in other codes
*******************************************************************************/
use define_tool_codes_aurum, clear
duplicates drop


* see if I can merge in the other codelists
local lists " "aurum_codelist_af" "
local lists " `lists' "aurum_codelist_cva" "
local lists " `lists' "aurum_codelist_mi" "
local lists " `lists' "aurum_codelist_pad" "
local lists " `lists' "aurum_codelist_tia" "
local lists " `lists' "aurum_codelist_ua" "
local lists " `lists' "aurum_codelist_vte" "
local lists " `lists' "aurum_codelist_hf" "
local lists " `lists' "aurum_codelist_ihd" "
local lists " `lists' "codelist_SMI_cprd_aurum" "
local lists " `lists' "depression_codelist_Aurum_v3" "
local lists " `lists' "cr_codelist_anxiety" "
local lists " `lists' "selfharm_codelist_Aurum_v1" "
local lists " `lists' "ED_codes" "
local lists " `lists' "OCD" "
local lists " `lists' "codelistAurum_respiratory_aeasthma" "
local lists " `lists' "codelistAurum_respiratory_aecopd" "
local lists " `lists' "codelistAurum_respiratory_asthmadenominator" "
local lists " `lists' "codelistAurum_respiratory_copddenominator" "
local lists " `lists' "diab_emergency_codes_rm_aurum" "
local lists " `lists' "Codelist_ethnicity_aurum_sml" "



local count=1

foreach x in `lists' {
	display in blue "`x'_new"
	merge 1:1 medcodeid using `x'_new.dta, force
	local name=abbrev("`x'",32)
	gen `name'=1 if _merge==3 | _merge==2
	drop _merge
}







/*******************************************************************************
#3. merge in clinical terms for code lists where this was missing
*******************************************************************************/





use cr_codelist_anxiety.dta, clear
gen anx=1

append using depression_codelist_Aurum_v3, force
gen dep=1 if anx==.

append using codelist_SMI_cprd_aurum, force
gen smi=1 if anx==. & dep==.
