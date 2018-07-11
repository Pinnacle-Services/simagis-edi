(: Ptn ANSI X12 835 :)
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
    let $payer_name := $trn/loop[@Id = "1000"]/segment[@Id = "N1" and *:element[@Id="N101"] = "PR"]/element[@Id = "N102"]/text()
    let $payer_id1 := $trn/loop[@Id = "1000"]/segment[@Id = "N1" and *:elementt[@Id="N101"] = "PR"]/element[@Id = "N104"]/text()
    let $payer_id2 := $trn/loop[@Id = "1000"]/segment[@Id = "REF" and *:element[@Id="REF01"] = "2U"]/element[@Id = "REF02"]/text()
    let $payer_id := functx:if-empty( data($payer_id2), data($payer_id1))
   (: Payer Contact:)
    let $payer_contact := $trn/loop[@Id = "1000"]/segment[@Id = "PER" and *:element[@Id="PER01"] = "CX"]/element[@Id = "PER02"]/text()
    let $payer_phone := $trn/loop[@Id = "1000"]/segment[@Id = "PER" and *:element[@Id="PER01"] = "CX"]/element[@Id = "PER04"]/text()
   (:Payment:)
    let $payby := $trn/segment[@Id="BPR"]/element[@Id="BPR04"]/text()
    let $paydt := $trn/segment[@Id="BPR"]/element[@Id="BPR16"]/text()
    let $payacct := $trn/segment[@Id="BPR"]/element[@Id="BPR15"]/text()
    let $checknum := $trn/segment[@Id="TRN"]/element[@Id="TRN02"]/text()
   (: Sender:)
    let $from_name := $trn/loop[@Id = "1000"]/segment[@Id = "N1" and *:element[@Id="N101"] = "PE"]/element[@Id = "N102"]/text()
    let $from_id := $trn/loop[@Id = "1000"]/segment[@Id = "N1" and *:element[@Id="N101"] = "PE"]/element[@Id = "N104"]/text()
   (: Accession Data :)
    for $clp in $trn/loop[@Id = "2000"]/loop[@Id = "2100"]
    let $acn_id := $clp/segment[@Id = "CLP"]/element[@Id = "CLP01"]/text()
    let $status := $clp/segment[@Id = "CLP"]/element[@Id = "CLP02"]/text()
    let $ref := $clp/segment[@Id = "CLP"]/element[@Id = "CLP07"]/text()
    let $filing := $clp/segment[@Id = "CLP"]/element[@Id = "CLP06"]/text()
      
     (: ptn data:)
     let $ptn_Id := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "QC"]/element[@Id = "NM109"]/text()
     let $ptn_IdType := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "QC"]/element[@Id = "NM108"]/text()
     let $ptn_Suff := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "QC"]/element[@Id = "NM107"]/text()
     let $ptn_Pref := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "QC"]/element[@Id = "NM106"]/text()
     let $ptn_MI := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "QC"]/element[@Id = "NM105"]/text()
     let $ptn_First := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "QC"]/element[@Id = "NM104"]/text()
     let $ptn_Last := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "QC"]/element[@Id = "NM103"]/text()
     let $ptn_Type := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "QC"]/element[@Id = "NM102"]/text()
     
     (: insured data:)
     let $insured_Id := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "IL"]/element[@Id = "NM109"]/text()
     let $insured_IdType := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "IL"]/element[@Id = "NM108"]/text()
     let $insured_Suff := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "IL"]/element[@Id = "NM107"]/text()
     let $insured_Pref := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "IL"]/element[@Id = "NM106"]/text()
     let $insured_MI := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "IL"]/element[@Id = "NM105"]/text()
     let $insured_First := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "IL"]/element[@Id = "NM104"]/text()
     let $insured_Last := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "IL"]/element[@Id = "NM103"]/text()
     let $insured_Type := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "IL"]/element[@Id = "NM102"]/text()

     (: corrected insured data:)
     let $corrected_Id := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "74"]/element[@Id = "NM109"]/text()
     let $corrected_IdType := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "74"]/element[@Id = "NM108"]/text()
     let $corrected_Suff := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "74"]/element[@Id = "NM107"]/text()
     let $corrected_Pref := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "74"]/element[@Id = "NM106"]/text()
     let $corrected_MI := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "74"]/element[@Id = "NM105"]/text()
     let $corrected_First := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "74"]/element[@Id = "NM104"]/text()
     let $corrected_Last := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "74"]/element[@Id = "NM103"]/text()
     let $corrected_Type := $clp/segment[@Id = "NM1" and *:element[@Id = "NM101"] = "74"]/element[@Id = "NM102"]/text()
    
    return
        <_ type='object'> {
            <id>{concat($acn_id, "-R-", $ref)}</id>,
            <acn>{$acn_id}</acn>, 
            <ref>{$ref}</ref>,
            <procDate-DT8>{$date_tr}</procDate-DT8>,
            <frmn-CC>{functx:if-empty($from_name,"Empty")}</frmn-CC>,
            <frmid>{functx:if-empty($from_id,"Empty")}</frmid>,
            <prid>{functx:if-empty($payer_id,"Empty")}</prid>,
            <prn-CC>{$payer_name}</prn-CC>,
            <fCode>{$filing}</fCode>
            
            (:ptn fields:)
            ,<ptnId>{$ptn_Id}</ptnId>
            ,<ptnIdType>{$ptn_IdType}</ptnIdType>
            ,<ptnFirst>{$ptn_First}</ptnFirst>
            ,<ptnLast>{$ptn_Last}</ptnLast>
            ,<ptnMI>{$ptn_MI}</ptnMI>
            ,<ptnPref>{$ptn_Pref}</ptnPref>
            ,<ptnSuff>{$ptn_Suff}</ptnSuff>
            ,<ptnType>{$ptn_Type}</ptnType>
            
            (:insured fields:)
            ,<insuredId>{$insured_Id}</insuredId>
            ,<insuredIdType>{$insured_IdType}</insuredIdType>
            ,<insuredFirst>{$insured_First}</insuredFirst>
            ,<insuredLast>{$insured_Last}</insuredLast>
            ,<insuredMI>{$insured_MI}</insuredMI>
            ,<insuredPref>{$insured_Pref}</insuredPref>
            ,<insuredSuff>{$insured_Suff}</insuredSuff>
            ,<insuredType>{$insured_Type}</insuredType> 
            
            (:corrected fields:)
            ,<correctedId>{$corrected_Id}</correctedId>
            ,<correctedIdType>{$corrected_IdType}</correctedIdType>
            ,<correctedFirst>{$corrected_First}</correctedFirst>
            ,<correctedLast>{$corrected_Last}</correctedLast>
            ,<correctedMI>{$corrected_MI}</correctedMI>
            ,<correctedPref>{$corrected_Pref}</correctedPref>
            ,<correctedSuff>{$corrected_Suff}</correctedSuff>
            ,<correctedType>{$corrected_Type}</correctedType>           
            
        }</_>
}
</json>