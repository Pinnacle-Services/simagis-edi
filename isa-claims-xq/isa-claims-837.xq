declare option output:method "json" ;
<json type='array'>{
for $grp in collection()//group
(: transmission date:)
let $date_tr := data($grp/@Date)
let $control := data($grp/@Control)
for $trn in $grp/transaction

(: Receiver :)
let $rec_name := $trn/loop[@Id="1000"]/segment[@Id="NM1" and *:element="40"]/element[@Id="NM103"]/text()
let $rec_id := $trn/loop[@Id="1000"]/segment[@Id="NM1" and *:element="40"]/element[@Id="NM109"]/text()
for $bl in $trn/loop[@Id="2000"]

(: Payer:)
let $payer_name := $bl/loop[@Id="2010"]//segment[@Id="NM1" and *:element="PR"]/element[@Id="NM103"]/text()
let $payer_id := $bl/loop[@Id="2010"]//segment[@Id="NM1" and *:element="PR"]/element[@Id="NM109"]/text()

(:Claim:)
for $clm in $bl/loop[@Id=2300]
let $acn_id:= $clm/segment[@Id="CLM"]/element[@Id="CLM01"]/text()
let $freq :=  $clm/segment[@Id="CLM"]/element[@Id="CLM05"]/subelement[@Sequence =3]/text()
let $ask_amt:= $clm/segment[@Id="CLM"]/element[@Id="CLM02"]/text()

(:billing system:)
let $sys := "U"

(: Dr :)
let $dr_last := $clm/loop[@Id="2310"]/segment[@Id="NM1" and *:element[@Id="NM101" and .="DN"]]/element[@Id="NM103"]/text()
let $dr_first := $clm/loop[@Id="2310"]/segment[@Id="NM1" and *:element[@Id="NM101" and .="DN"]]/element[@Id="NM104"]/text()
let $dr_npi := $clm/loop[@Id="2310"]/segment[@Id="NM1" and *:element[@Id="NM101" and .="DN"]]/element[@Id="NM109"]/text()

return 
<_ type='object'> {

(: header data :)
<id>{concat($acn_id,"-C-", $control)}</id>,
<acn>{$acn_id}</acn>,
<freq>{$freq}</freq>,
<sendDate-DT8>{$date_tr}</sendDate-DT8>,
<recN-CC>{ $rec_name}</recN-CC>,
<recId>{ $rec_id}</recId>,
<prn-CC>{$payer_name}</prn-CC>,
<prid>{$payer_id}</prid>,
<clmAsk-C0>{ $ask_amt}</clmAsk-C0>,

(:billing system:)
if (matches($acn_id, "^\D{3}\d{9}"))
then <sys>{"V"}</sys>
else if (matches($acn_id,"^\d{9}-\d{4}\D"))
then <sys>{"X"}</sys>
else if (matches($acn_id,"^GN\d{5}"))
then <sys>{"Q"}</sys>
else <sys>{"U"}</sys>,

(:Dr Data:)
<npi>{$dr_npi}</npi>,
<drLastN-CC>{$dr_last}</drLastN-CC>,
<drFirsN-CC>{ $dr_first}</drFirsN-CC>,

(: DX Data:)
<dx type = 'array'>{
  for $dx in $clm/segment[@Id="HI"]/element
  let $dx_type := $dx/subelement[@Sequence="1"]/text()
  let $dx_value := $dx/subelement[@Sequence="2"]/text()
  return <_ type='object'>{
    <dxT>{$dx_type}</dxT>,
    <dxV>{$dx_value}</dxV>  
  }</_>
  
}</dx>,

(: CPT Information:)
<svc type= 'array'>{
for $svc in $clm/loop[@Id="2400"]
let $cpt := $svc/segment[@Id="SV1"]/element[@Id="SV101"]/subelement[@Sequence=2]/text()
let $mod := $svc/segment[@Id="SV1"]/element[@Id="SV101"]/subelement[@Sequence=3]/text()
let $cpt_descr := $svc/segment[@Id="SV1"]/element[@Id="SV101"]/subelement[@Sequence=7]/text()
let $cpt_ask :=  $svc/segment[@Id="SV1"]/element[@Id="SV102"]/text()
let $qty :=  $svc/segment[@Id="SV1"]/element[@Id="SV104"]/text()
let $svc_loc :=  $svc/segment[@Id="SV1"]/element[@Id="SV105"]/text()

(:service date info:)
let $svc_dt_type:= $svc/segment[@Id="DTP" and *:element[@Id="DTP01" and .=472]]/element[@Id="DTP02"]/text()
let $svc_dt:= $svc/segment[@Id="DTP" and *:element[@Id="DTP01" and .=472]]/element[@Id="DTP03"]/text()

  return
  <_ type = 'object'>{
    if ( $svc_dt_type = 'RD8')
    then <srvDate-DT8>{ replace($svc_dt, "^\d{8}-","" ) }</srvDate-DT8>
    else <srvDate-DT8>{$svc_dt}</srvDate-DT8>,
    <cpt >{$cpt}</cpt>,
    <cptMod1>{$mod}</cptMod1>,
    <cptId >{concat($cpt,$mod)}</cptId>,
    <cptFullId >{concat($acn_id,$cpt,$mod)}</cptFullId>,
    <cptDsc>{$cpt_descr}</cptDsc>,
    <qty-I>{$qty}</qty-I>,
    <cptAsk-C0>{$cpt_ask}</cptAsk-C0>
    
  }</_>
  
}</svc>

}
</_>
}</json>
