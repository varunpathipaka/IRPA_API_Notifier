const cds = require('@sap/cds');
const axios = require('axios');

module.exports = cds.service.impl(async function () {
var finalresponse = {};
    this.on('botcall', async (req) => {
       // console.log(JSON.stringify(req.data));
if(finalresponse.length)
{
    return finalresponse;
}

        let token = await axios.request({
            url: "/oauth/token?grant_type=client_credentials",
            method: "post",
            baseURL: "https://trail-9ccxobwy.authentication.us10.hana.ondemand.com",
            auth: {
                username: "sb-e78adcd5-d974-43d2-b229-4f4afdb59e8e!b45919|sapmlirpa--irpatrial--trial--uaa-service-broker!b3516",
                password: "a797b495-7ea0-4461-8a9b-8e85b1b87dd4$BvvPYLXqXb2pRd44PtMvg1jggUAWOnAkOTX49wHbGGM="
            }
        }).then(function (res) {
            return res.data.access_token;
            console.log(res);
        }).catch((error) => {
            console.log(error);
        });

        let payload = {
            "invocationContext": "${invocation_context}",
            "input": {
                "data": [
                    {
                        "to_Item":
                        {
                            "SalesOrder": "5000021",
                            "Material": "MATERIAL_Z",
                            "RequestedQuantity": "10",
                            "ShippingType": "04",
                            "DeliveryPriority": "02"
                        },
                        "SalesOrder": "5000021",
                        "SalesOrderType": "OR",
                        "SalesOrganization": "1100",
                        "DistributionChannel": "01",
                        "OrganizationDivision": "01",
                        "PurchaseOrderByCustomer": "PO_001",
                        "SoldToParty": "200347",
                        "PaymentMethod": "D",
                        "ShippingCondition": "04"
                    },

                ]
            }
        }


        let botcallResponse = await axios.request({
            url: `https://api.irpa-trial.cfapps.us10.hana.ondemand.com/runtime/v1/apiTriggers/562ab2e9-ae4d-4a5f-a8a6-b355926ed746/runs`,
            method: "post",
            headers: {
                'Authorization': 'Bearer ' + token,
                'irpa-api-key': "AZTSa8GbApLGjqMQo1OEt31_PY0X0_HY",
                'Content-Type': "application/json"
            },
            data: payload
        }).then((res) => {
                return res.data.jobUid
            }).catch((err) => {
                return err.response.status
            });

        return "";
    });
    this.on('botResponse', async(req) => {
        let response = req.data.output;
        console.log(JSON.stringify(req.data));
        finalresponse = response;
        return response;
    });
    this.on('botfinalresponse', async(req) => {
      
        return finalresponse;
    });
});