{
  "requiredNodes": [
    {
      "nodeName": "SRC_DBSHIFT_DBSHIFT_SALESORDERHEADER",
      "nodeType": "Source",
      "dependencies": []
    },
    {
      "nodeName": "STG_DBSHIFT_DBSHIFT_SALESORDERHEADER",
      "nodeType": "Stage",
      "dependencies": [
        "SRC_DBSHIFT_DBSHIFT_SALESORDERHEADER"
      ]
    },
    {
      "nodeName": "SRC_DBSHIFT_DBSHIFT_SHIPPINGINFO",
      "nodeType": "Source",
      "dependencies": []
    },
    {
      "nodeName": "STG_DBSHIFT_DBSHIFT_SHIPPINGINFO",
      "nodeType": "Stage",
      "dependencies": [
        "SRC_DBSHIFT_DBSHIFT_SHIPPINGINFO"
      ]
    },
    {
      "nodeName": "SRC_DBSHIFT_DBSHIFT_PRIORITYATTRIBUTES",
      "nodeType": "Source",
      "dependencies": []
    },
    {
      "nodeName": "STG_DBSHIFT_DBSHIFT_PRIORITYATTRIBUTES",
      "nodeType": "Stage",
      "dependencies": [
        "SRC_DBSHIFT_DBSHIFT_PRIORITYATTRIBUTES"
      ]
    },
    {
      "nodeName": "SRC_DBSHIFT_DBSHIFT_TRANSFERREDORDERS",
      "nodeType": "Source",
      "dependencies": []
    },
    {
      "nodeName": "STG_DBSHIFT_DBSHIFT_TRANSFERREDORDERS",
      "nodeType": "Stage",
      "dependencies": [
        "SRC_DBSHIFT_DBSHIFT_TRANSFERREDORDERS"
      ]
    },
    {
      "nodeName": "STG_FACT_SALES_AGGREGATE_LOOKUP",
      "nodeType": "Stage",
      "dependencies": [
        "STG_DBSHIFT_DBSHIFT_SALESORDERHEADER",
        "STG_DBSHIFT_DBSHIFT_SHIPPINGINFO",
        "STG_DBSHIFT_DBSHIFT_PRIORITYATTRIBUTES",
        "STG_DBSHIFT_DBSHIFT_TRANSFERREDORDERS"
      ]
    },
    {
      "nodeName": "FCT_SALES_AGGREGATE",
      "nodeType": "Fact",
      "dependencies": [
        "STG_FACT_SALES_AGGREGATE_LOOKUP"
      ]
    },
    {
      "nodeName": "V_FCT_SALES_AGGREGATE",
      "nodeType": "View",
      "dependencies": [
        "FCT_SALES_AGGREGATE"
      ]
    }
  ]
}