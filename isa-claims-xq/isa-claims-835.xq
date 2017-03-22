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
    (: Sender:)
    let $from_name := $trn/loop[@Id = "1000"]/segment[@Id = "N1" and *:element = "PE"]/element[@Id = "N102"]/text()
    let $from_id := $trn/loop[@Id = "1000"]/segment[@Id = "N1" and *:element = "PE"]/element[@Id = "N104"]/text()
    (: Accession Data :)
    for $clp in $trn/loop[@Id = "2000"]/loop[@Id = "2100"]
    let $acn_id := $clp/segment[@Id = "CLP"]/element[@Id = "CLP01"]/text()
    let $status := $clp/segment[@Id = "CLP"]/element[@Id = "CLP02"]/text()
    let $ref := $clp/segment[@Id = "CLP"]/element[@Id = "CLP07"]/text()
    let $loc := $clp/segment[@Id = "CLP"]/element[@Id = "CLP08"]/text()
    let $freq := $clp/segment[@Id = "CLP"]/element[@Id = "CLP09"]/text()
    let $ask_amt := data($clp/segment[@Id = "CLP"]/element[@Id = "CLP03"])
    let $pay_amt := data($clp/segment[@Id = "CLP"]/element[@Id = "CLP04"])
    let $pr := functx:if-empty( data($clp/segment[@Id = "CLP"]/element[@Id = "CLP05"]), 0.00)
    let $total_pay := fn:sum((number($pr), number($pay_amt)))
    let $filing := $clp/segment[@Id = "CLP"]/element[@Id = "CLP06"]/text()
    return
        <_ type='object'> {
            <id>{concat($acn_id, "-R-", $ref)}</id>,
            <acn>{$acn_id}</acn>,
            
            (:billing system:)
            if (matches($acn_id, "^\D{3}\d{9}"))
            then <sys>{"V"}</sys>
            else if (matches($acn_id,"^\d{9}-\d{4}\D"))
            then <sys>{"X"}</sys>
            else if (matches($acn_id,"^GN\d{5}"))
            then <sys>{"Q"}</sys>
            else <sys>{"U"}</sys>,                  
            
            <ref>{$ref}</ref>,
            <status>{$status}</status>,
            <procDate-DT8>{$date_tr}</procDate-DT8>,
            <payBy>{$payby}</payBy>,
            <payDate-DT8>{$paydt}</payDate-DT8>,
            <loc>{$loc}</loc>,
            <freq>{$freq}</freq>,     
            <frmn-CC>{functx:if-empty($from_name,"Empty")}</frmn-CC>,
            <frmid>{functx:if-empty($from_id,"Empty")}</frmid>,
            <prid>{functx:if-empty($payer_id,"Empty")}</prid>,
            <prn-CC>{$payer_name}</prn-CC>,
            <fCode>{$filing}</fCode>,
            <clmAsk-C0>{$ask_amt}</clmAsk-C0>,
            <clmPay-C0>{$pay_amt}</clmPay-C0>,           
            <pr-C0>{$pr}</pr-C0>,
            <clmPayTotal-C0>{fn:round($total_pay, 2)}</clmPayTotal-C0>,
                 
            (:Claim Remarks:)            
            <remarks type='array'>{
              for $rem in $clp/segment[@Id="MOA" or @Id="MIA"]/element
              return
              <_ type = 'object'>{
                <rem>{$rem/text()}</rem>
              }</_>
            }</remarks>,
            
            (: CPT Level:)
            <svc type='array'>{
                for $cpt in $clp/loop[@Id = "2110"]
                let $cpt_id := $cpt/segment[@Id = "SVC"]/element[@Id = "SVC01"]/subelement[@Sequence = "2"]/text()
                let $cpt_mod := $cpt/segment[@Id = "SVC"]/element[@Id = "SVC01"]/subelement[@Sequence = "3"]/text()
                let $cpt_mod2 := $cpt/segment[@Id = "SVC"]/element[@Id = "SVC01"]/subelement[@Sequence = "4"]/text()
                let $cpt_mod3 := $cpt/segment[@Id = "SVC"]/element[@Id = "SVC01"]/subelement[@Sequence = "4"]/text()
                let $cpt_ask := $cpt/segment[@Id = "SVC"]/element[@Id = "SVC02"]/text()
                let $cpt_pay := $cpt/segment[@Id = "SVC"]/element[@Id = "SVC03"]/text()
                let $cpt_qty := $cpt/segment[@Id = "SVC"]/element[@Id = "SVC05"]/text()
                (: Dates:)
                let $date_srv := $cpt/segment[@Id = "DTM" and *:element[. = "472"]]/element[@Id = "DTM02"]/text()

                return
                    <_ type='object'>{
                        <cptId >{concat($cpt_id,$cpt_mod)}</cptId>,
                        <cptFullId >{concat($acn_id, $cpt_id, $cpt_mod)}</cptFullId>,
                        <cpt >{$cpt_id}</cpt>,
                        <cptMod1>{$cpt_mod}</cptMod1>,
                        <mod2>{$cpt_mod2}</mod2>,
                        <mod3>{$cpt_mod3}</mod3>,
                        <qty-C0>{$cpt_qty}</qty-C0>,
                        <cptAsk-C0>{$cpt_ask}</cptAsk-C0>,
                        <cptPay-C0>{$cpt_pay}</cptPay-C0>,
                        <srvDate-DT8>{$date_srv[1]}</srvDate-DT8>,
                        (: adjustments information:)
                        <adj type = 'array'> {
                        for $adj in $cpt/segment[@Id="CAS"]
                        let $adj_group := $adj/element[@Id="CAS01"]/text()
                        let $adj_reason := $adj/element[@Id="CAS02"]/text()
                        let $adj_amount := $adj/element[@Id="CAS03"]/text()
                        return 
                            <_ type='object'>{
                            <adjGrp>{$adj_group}</adjGrp>,
                            <adjReason>{$adj_reason}</adjReason>,
                            <adjAmt-C0>{$adj_amount}</adjAmt-C0>
                            }</_>
                        }</adj>
                    } </_>
            }</svc>
        }</_>
}
</json>
