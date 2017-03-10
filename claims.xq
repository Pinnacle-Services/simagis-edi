declare option output:method "json";
<json type='array'>{
    for $grp in collection()//group
    (: transmission date:)
    let $date_tr := data($grp/@Date)
    for $trn in $grp/transaction
    (: Payer:)
    let $payer_name := $trn/loop[@Id = "1000"]/segment[@Id = "N1" and *:element = "PR"]/element[@Id = "N102"]/text()
    let $payer_id := $trn/loop[@Id = "1000"]/segment[@Id = "N1" and *:element = "PR"]/element[@Id = "N104"]/text()
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
    let $ask_amt := $clp/segment[@Id = "CLP"]/element[@Id = "CLP03"]/text()
    let $pay_amt := $clp/segment[@Id = "CLP"]/element[@Id = "CLP04"]/text()
    let $pr := $clp/segment[@Id = "CLP"]/element[@Id = "CLP05"]/text()
    let $filing := $clp/segment[@Id = "CLP"]/element[@Id = "CLP06"]/text()
    return
        <_ type='object'> {
            <id>{concat($acn_id, "-R-", $ref)}</id>,
            <acn>{$acn_id}</acn>,
            <ref>{$ref}</ref>,
            <status>{$status}</status>,
            <proc-DT8>{$date_tr}</proc-DT8>,
            <loc>{$loc}</loc>,
            <freq>{$freq}</freq>,
            <frmn>{$from_name}</frmn>,
            <frmid>{$from_id}</frmid>,
            <prid>{$payer_id}</prid>,
            <prn>{$payer_name}</prn>,
            <fl-I>{$filing}</fl-I>,
            <clmAsk-F>{$ask_amt}</clmAsk-F>,
            <clmPay-F>{$pay_amt}</clmPay-F>,
            <pr-F>{$pr}</pr-F>,
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
                        <cpt >{$cpt_id}</cpt>,
                        <mod1>{$cpt_mod}</mod1>,
                        <mod2 >{$cpt_mod2}</mod2>,
                        <mod3 >{$cpt_mod3}</mod3>,
                        <qty >{$cpt_qty}</qty>,
                        <ask-F>{$cpt_ask}</ask-F>,
                        <pay-F>{$cpt_ask}</pay-F>,
                        <srv-DT8 >{$date_srv[1]}</srv-DT8>
                    } </_>
            }</svc>
        }</_>
}
</json>



