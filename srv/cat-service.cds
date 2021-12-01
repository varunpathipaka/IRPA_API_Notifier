using my.bookshop as my from '../db/data-model';

service CatalogService {
    @readonly
    entity Books as projection on my.Books;
    function botcall() returns repsonseType;
    function botfinalresponse() returns repsonseType;
    type repsonseType {
            SalesOrder: String;
            Material: String;
            RequestedQuantity: String;
            ShippingType: String;
            DeliveryPriority: String;
    }
    type responsefinal
    {
    output1 : array of repsonseType 
    }
    action botResponse(output : responsefinal) returns responsefinal;
}