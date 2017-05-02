(: process 835 EDI ERA docs:)
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
    for $trn in $grp/transaction
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
    let $plb31 := $plb/element[@Id = "PLB03"]/subelement[@Sequence="1"]/text()
    let $plb32 := $plb/element[@Id = "PLB03"]/subelement[@Sequence="2"]/text()
    let $plb4 := $plb/element[@Id = "PLB04"]/text()
 
    return
        <_ type='object'> {
            <procDate-DT8>{$date_tr}</procDate-DT8>,
            <payBy>{$payby}</payBy>,
            <accNum>{$payacct}</accNum>,
            <payID>{$checknum}</payID>,            
            <payDate-DT8>{$paydt}</payDate-DT8>,
            <frmn-CC>{functx:if-empty($from_name,"Empty")}</frmn-CC>,
            <frmid>{functx:if-empty($from_id,"Empty")}</frmid>,
            <prid>{functx:if-empty($payer_id,"Empty")}</prid>,
            <prn-CC>{$payer_name}</prn-CC>,
            <plb1>{$plb1}</plb1>,
            <plb2-DT8>{$plb2}</plb2-DT8>,
            <plb31>{$plb31}</plb31>,
            <plb32>{$plb32}</plb32>,
            <plb4-C0>{$plb4}</plb4-C0>       
        }</_>
}
</json>
