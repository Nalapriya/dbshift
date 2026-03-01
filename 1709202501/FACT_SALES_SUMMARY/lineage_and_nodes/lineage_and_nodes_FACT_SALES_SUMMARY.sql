{
  "requiredNodes": [
    {
      "nodeName": "SRC_ALCOBEVDB_ALCOBEV_PRIMARY_SALES_ACTUALS",
      "nodeType": "Source",
      "dependencies": []
    },
    {
      "nodeName": "STG_ALCOBEVDB_ALCOBEV_PRIMARY_SALES_ACTUALS",
      "nodeType": "Stage",
      "dependencies": [
        "SRC_ALCOBEVDB_ALCOBEV_PRIMARY_SALES_ACTUALS"
      ]
    },
    {
      "nodeName": "SRC_ALCOBEVDB_ALCOBEV_COMPANYMASTER",
      "nodeType": "Source",
      "dependencies": []
    },
    {
      "nodeName": "STG_ALCOBEVDB_ALCOBEV_COMPANYMASTER",
      "nodeType": "Stage",
      "dependencies": [
        "SRC_ALCOBEVDB_ALCOBEV_COMPANYMASTER"
      ]
    },
    {
      "nodeName": "SRC_ALCOBEVDB_ALCOBEV_GEOGRAPHYMASTER",
      "nodeType": "Source",
      "dependencies": []
    },
    {
      "nodeName": "STG_ALCOBEVDB_ALCOBEV_GEOGRAPHYMASTER",
      "nodeType": "Stage",
      "dependencies": [
        "SRC_ALCOBEVDB_ALCOBEV_GEOGRAPHYMASTER"
      ]
    },
    {
      "nodeName": "SRC_ALCOBEVDB_ALCOBEV_PRODUCTMASTER",
      "nodeType": "Source",
      "dependencies": []
    },
    {
      "nodeName": "STG_ALCOBEVDB_ALCOBEV_PRODUCTMASTER",
      "nodeType": "Stage",
      "dependencies": [
        "SRC_ALCOBEVDB_ALCOBEV_PRODUCTMASTER"
      ]
    },
    {
      "nodeName": "SRC_ALCOBEVDB_ALCOBEV_PRIMARY_SALES_PLAN_AOP",
      "nodeType": "Source",
      "dependencies": []
    },
    {
      "nodeName": "STG_ALCOBEVDB_ALCOBEV_PRIMARY_SALES_PLAN_AOP",
      "nodeType": "Stage",
      "dependencies": [
        "SRC_ALCOBEVDB_ALCOBEV_PRIMARY_SALES_PLAN_AOP"
      ]
    },
    {
      "nodeName": "SRC_ALCOBEVDB_ALCOBEV_OUTLETMASTER",
      "nodeType": "Source",
      "dependencies": []
    },
    {
      "nodeName": "STG_ALCOBEVDB_ALCOBEV_OUTLETMASTER",
      "nodeType": "Stage",
      "dependencies": [
        "SRC_ALCOBEVDB_ALCOBEV_OUTLETMASTER"
      ]
    },
    {
      "nodeName": "SRC_ALCOBEVDB_ALCOBEV_ACTIVATIONMASTER",
      "nodeType": "Source",
      "dependencies": []
    },
    {
      "nodeName": "STG_ALCOBEVDB_ALCOBEV_ACTIVATIONMASTER",
      "nodeType": "Stage",
      "dependencies": [
        "SRC_ALCOBEVDB_ALCOBEV_ACTIVATIONMASTER"
      ]
    },
    {
      "nodeName": "STG_FACT_SALES_SUMMARY_PREP",
      "nodeType": "Stage",
      "dependencies": [
        "STG_ALCOBEVDB_ALCOBEV_PRIMARY_SALES_ACTUALS",
        "STG_ALCOBEVDB_ALCOBEV_COMPANYMASTER",
        "STG_ALCOBEVDB_ALCOBEV_GEOGRAPHYMASTER",
        "STG_ALCOBEVDB_ALCOBEV_PRODUCTMASTER",
        "STG_ALCOBEVDB_ALCOBEV_PRIMARY_SALES_PLAN_AOP",
        "STG_ALCOBEVDB_ALCOBEV_OUTLETMASTER",
        "STG_ALCOBEVDB_ALCOBEV_ACTIVATIONMASTER"
      ]
    },
    {
      "nodeName": "FCT_SALES_SUMMARY",
      "nodeType": "Fact",
      "dependencies": [
        "STG_FACT_SALES_SUMMARY_PREP"
      ]
    },
    {
      "nodeName": "V_FCT_SALES_SUMMARY",
      "nodeType": "View",
      "dependencies": [
        "FCT_SALES_SUMMARY"
      ]
    }
  ]
}