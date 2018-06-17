(: process 835 EDI ERA docs:)
(: Provider Level Balances PLB :)
declare namespace functx = "http://www.functx.com";
declare function functx:if-empty
  ( $arg as item()? ,
    $value as item()* )  as item()* {

  if (string($arg) != '')
  then data($arg)
  else $value
 } ;

declare option output:method "json";

<json type='array'>{
    for $grp in collection()//group
    (: transmission date:)
    let $date_tr := data($grp/@Date)
    let $control_grp := data($grp/@Control)
    for $trn in $grp/transaction
    let $control_trn := data($trn/@Control)
    (: Payer:)
    let $payer_name := $trn/loop[@Id = "1000"]/segment[@Id = "N1" and *:element = "PR"]/element[@Id = "N102"]/text()
    let $payer_id := $trn/loop[@Id = "1000"]/segment[@Id = "N1" and *:element = "PR"]/element[@Id = "N104"]/text()
    (:Payment:)
    let $payby := $trn/segment[@Id="BPR"]/element[@Id="BPR04"]/text()
    let $paydt := $trn/segment[@Id="BPR"]/element[@Id="BPR16"]/text()
    let $payacct := $trn/segment[@Id="BPR"]/element[@Id="BPR15"]/text()
    let $checknum := $trn/segment[@Id="TRN"]/element[@Id="TRN02"]/text()
    (: Sender:)
    let $from_name := $trn/loop[@Id = "1000"]/segment[@Id = "N1" and *:element = "PE"]/element[@Id = "N102"]/text()
    let $from_id := $trn/loop[@Id = "1000"]/segment[@Id = "N1" and *:element = "PE"]/element[@Id = "N104"]/text()
    (: Provider Level Balances - PLB :)
    for $plb in $trn/segment[@Id = "PLB"]
    let $plb1 := $plb/element[@Id="PLB01"]/text()
    let $plb2 := $plb/element[@Id = "PLB02"]/text()
    let $plb30 := $plb/element[@Id = "PLB03"]/text() (: top value if not composite:)
    let $plb31 := $plb/element[@Id = "PLB03"]/subelement[@Sequence="1"]/text()
    let $plb32 := $plb/element[@Id = "PLB03"]/subelement[@Sequence="2"]/text()
    let $plb4 := $plb/element[@Id = "PLB04"]/text()
 
    return
        <_ type='object'> {
            <id>{concat($date_tr,"-", $control_grp, "-", $control_trn, "-", $plb30, $plb31)}</id>,
            <procDate-DT8>{$date_tr}</procDate-DT8>,
            <prn-CC>{$payer_name}</prn-CC>,
            <prid>{functx:if-empty($payer_id,"Empty")}</prid>,
            <frmid>{functx:if-empty($from_id,"Empty")}</frmid>,
            <provNm-CC>{functx:if-empty($from_name,"Empty")}</provNm-CC>,
            <provId>{$plb1}</provId>,
            <fiscalDt-DT8>{$plb2}</fiscalDt-DT8>,
            <adjRsn>{functx:if-empty($plb30, $plb31)}</adjRsn>,
            <ajdRef>{functx:if-empty($plb32,"Empty")}</ajdRef>,
            <ajdAmt-C0>{$plb4}</ajdAmt-C0>,
            <payBy>{$payby}</payBy>,
            <accNum>{$payacct}</accNum>,
            <payID>{$checknum}</payID>,            
            <payDate-DT8>{$paydt}</payDate-DT8>     
        }</_>
}
</json>
