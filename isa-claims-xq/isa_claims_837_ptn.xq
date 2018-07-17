(: ptn data from 837 doc:)
declare namespace functx = "http://www.functx.com";
declare function functx:if-empty
  ( $arg as item()? ,
    $value as item()* )  as item()* {

  if (string($arg) != '')
  then data($arg)
  else $value
 } ;

declare option output:method "json" ;
<json type='array'>{
for $grp in collection()//group
(: transmission date:)
let $date_tr := data($grp/@Date)
let $control := data($grp/@Control)

(:Transaction loop:)
for $trn in $grp/transaction

(: Receiver :)
let $rec_name := $trn/loop[@Id="1000"]/segment[@Id="NM1" and *:element="40"]/element[@Id="NM103"]/text()
let $rec_id := $trn/loop[@Id="1000"]/segment[@Id="NM1" and *:element="40"]/element[@Id="NM109"]/text()

(:Billing Loop:)
for $bl in $trn/loop[@Id="2000"]

(: Billing Provider :)
let $bill_name := $bl/loop[@Id="2010"]/segment[@Id="NM1" and element[@Id="NM101" and .="85"]]/element[@Id="NM103"]/text()
let $bill_id := $bl/loop[@Id="2010"]/segment[@Id="NM1" and element[@Id="NM101" and .="85"]]/element[@Id="NM109"]/text()
let $ein := $bl/loop[@Id="2010"]/segment[@Id="REF" and element[@Id="REF01" and .="EI"]]/element[@Id="REF02"]/text()
  
(: Subscriber Info:)
for $sbr in $bl/segment[@Id="SBR"]
let $sbr_responsibility := $sbr/element[@Id="SBR01"]/text()
let $sbr_relationships := $sbr/element[@Id="SBR02"]/text()
let $sbr_policyId := $sbr/element[@Id="SBR03"]/text()
let $sbr_groupName := $sbr/element[@Id="SBR04"]/text()
let $sbr_insType := $sbr/element[@Id="SBR05"]/text()
let $sbr_coob := $sbr/element[@Id="SBR06"]/text()
let $sbr_yesNo := $sbr/element[@Id="SBR07"]/text()
let $sbr_employmentStatus := $sbr/element[@Id="SBR08"]/text()
let $sbr_fCode := $sbr/element[@Id="SBR09"]/text()
     
(: Subscriber Loop:)
for $sbrl in $bl//loop[@Id="2010" and segment[@Id="NM1" and *:element="IL"] ]

    (: Subscriber Name:)
    for $sbr_name in $sbrl/segment[@Id="NM1" and element[@Id="NM101" and .="IL"]]
         let $insured_Id := $sbr_name//element[@Id = "NM108"]/text()
         let $insured_Suff := $sbr_name//element[@Id = "NM107"]/text()
         let $insured_Pref := $sbr_name//element[@Id = "NM106"]/text()
         let $insured_MI := $sbr_name//element[@Id = "NM105"]/text()
         let $insured_First := $sbr_name//element[@Id = "NM104"]/text()
         let $insured_Last := $sbr_name//element[@Id = "NM103"]/text()
         let $insured_Type := $sbr_name//element[@Id = "NM102"]/text()
    
    (: Demographics:)
    let $ptn_bd := $sbrl/segment[@Id="DMG"]/element[@Id="DMG02"]/text()
    let $ptn_g := $sbrl/segment[@Id="DMG"]/element[@Id="DMG03"]/text()
    
    (: Subsriber Address:)
    let $sbr_addr1 := $sbrl/segment[@Id="N3"]/element[@Id="N301"]/text()
    let $sbr_addr2 := $sbrl/segment[@Id="N3"]/element[@Id="N302"]/text()
    let $sbr_city := $sbrl/segment[@Id="N4"]/element[@Id="N401"]/text()
    let $sbr_state := $sbrl/segment[@Id="N4"]/element[@Id="N402"]/text()
    let $sbr_zip := $sbrl/segment[@Id="N4"]/element[@Id="N403"]/text()


(: Payer:)
let $payer_name := $bl/loop[@Id="2010"]//segment[@Id="NM1" and *:element="PR"]/element[@Id="NM103"]/text()
let $payer_id := $bl/loop[@Id="2010"]//segment[@Id="NM1" and *:element="PR"]/element[@Id="NM109"]/text()

(:Claim:)
for $clm in $bl/loop[@Id=2300]
let $acn_id:= $clm/segment[@Id="CLM"]/element[@Id="CLM01"]/text()

(:billing system:)
let $sys := "U"

return 
<_ type='object'> {

(: header data :)
  <id>{concat($acn_id,"-C-", $control)}</id>,
  <acn>{$acn_id}</acn>,
  <sendDate-DT8>{$date_tr}</sendDate-DT8>,
  <recN-CC>{functx:if-empty($rec_name,"Empty")}</recN-CC>,
  <recId>{functx:if-empty($rec_id,"Empty")}</recId>,
  <prn-CC>{functx:if-empty($payer_name,"Empty")}</prn-CC>,
  <prid>{functx:if-empty($payer_id,"Empty")}</prid>,

(:Billing Provider:)
  <billN-CC>{functx:if-empty($bill_name[1],"Empty")}</billN-CC>,
  <billId>{functx:if-empty($bill_id[1],"Empty")}</billId>,
  <ein>{functx:if-empty($ein[1],"Empty")}</ein>

(: Subscriber Info:)
  ,<sbrResp>{$sbr_responsibility}</sbrResp>
  ,<sbrRel>{$sbr_relationships}</sbrRel>
  ,<sbrPolicyId>{$sbr_policyId}</sbrPolicyId>
  ,<sbrGroupName-CC>{$sbr_groupName }</sbrGroupName-CC>
  ,<sbrInsType>{$sbr_insType}</sbrInsType>  
  ,<sbrCOOB>{$sbr_coob}</sbrCOOB> 
  ,<sbrYN>{$sbr_yesNo}</sbrYN>
  ,<sbrEmplStat>{$sbr_employmentStatus}</sbrEmplStat>
  ,<sbrfCode>{$sbr_fCode}</sbrfCode>

     
(: Subscriber Name:)
  ,<sbrId>{$insured_Id}</sbrId>
  ,<sbrSfx-CC>{$insured_Suff}</sbrSfx-CC>
  ,<sbrPfx-CC>{$insured_Pref}</sbrPfx-CC>
  ,<sbrMI-CC>{$insured_MI}</sbrMI-CC>
  ,<sbrFirst-CC>{$insured_First}</sbrFirst-CC>
  ,<sbrLast-CC>{$insured_Last}</sbrLast-CC>
  ,<sbrType>{$insured_Type}</sbrType>
  
(: Demographics:)
  ,<ptnBd-DT8>{$ptn_bd}</ptnBd-DT8>
  ,<ptnG>{$ptn_g}</ptnG>

(: Subsriber Address:)
,<sbrAdr1-CC>{$sbr_addr1}</sbrAdr1-CC>
,<sbrAdr2-CC>{$sbr_addr2}</sbrAdr2-CC>
,<sbrCity-CC>{$sbr_city}</sbrCity-CC>
,<sbrSt>{$sbr_state}</sbrSt>
,<sbrZIP>{$sbr_zip}</sbrZIP>


}
</_>
}</json>
