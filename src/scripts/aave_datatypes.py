reserves_struct_v3 =  [
    {
        "campo": "configuration", 
        "tipo": "ReserveConfigurationMap", 
        "description": "stores the reserve configuration"
    },{
        "campo": "liquidityIndex",
        "tipo": "uint128",
        "description": "the liquidity index. Expressed in ray"
    },{
        "campo": "currentLiquidityRate", 
        "tipo": "uint128", 
        "description": "the current supply rate. Expressed in ray",
    },{
        "campo": "variableBorrowIndex",
        "tipo": "uint128",
        "description": "variable borrow index. Expressed in ray"
    },{
        "campo": "currentVariableBorrowRate", 
        "tipo": "uint128", 
        "description": "the current variable borrow rate. Expressed in ray"
    },{
        "campo": "currentStableBorrowRate",
        "tipo": "uint128",
        "description": "the current stable borrow rate. Expressed in ray"
    },{
        "campo": "lastUpdateTimestamp",
        "tipo": "uint40", 
        "description": "timestamp of last update"
    },{
        "campo": "id",
        "tipo": "uint16", 
        "description": "the id of the reserve. Represents the position in the list of the active reserves",
    },{
        "campo": "aTokenAddress",
        "tipo": "address",
        "description": "aToken address"
    },{
        "campo": "stableDebtTokenAddress",
        "tipo": "address",
        "description": "stableDebtToken address"
    },{
        "campo": "variableDebtTokenAddress",
        "tipo": "address", 
        "description": "variableDebtToken address"
    },{
        "campo": "interestRateStrategyAddress",
        "tipo": "address",
        "description": "address of the interest rate strategy"
    },{
        "campo": "accruedToTreasury",
        "tipo": "uint128",
        "description": "the current treasury balance, scaled"
    },{
        "campo": "unbacked",
        "tipo": "uint128",
        "description": "the outstanding unbacked aTokens minted through the bridging feature"
    },{
        "campo": "isolationModeTotalDebt",
        "tipo": "uint128",
        "description": "the outstanding debt borrowed against this asset in isolation mode"
    }
]


reserves_struct_v2 = [
    {
        "campo": "configuration", 
        "tipo": "ReserveConfigurationMap", 
        "description": "stores the reserve configuration"
    },{
        "campo": "liquidityIndex",
        "tipo": "uint128",
        "description": "the liquidity index. Expressed in ray"
    },{
        "campo": "variableBorrowIndex",
        "tipo": "uint128",
        "description": "variable borrow index. Expressed in ray"
    },{
        "campo": "currentLiquidityRate",
        "tipo": "uint128", 
        "description": "the current supply rate. Expressed in ray",
    },{
        "campo": "currentVariableBorrowRate", 
        "tipo": "uint128", 
        "description": "the current variable borrow rate. Expressed in ray"
    },{
        "campo": "currentStableBorrowRate",
        "tipo": "uint128",
        "description": "the current stable borrow rate. Expressed in ray"
    },{
        "campo": "lastUpdateTimestamp",
        "tipo": "uint40", 
        "description": "timestamp of last update"
    },{
        "campo": "aTokenAddress",
        "tipo": "address",
        "description": "aToken address"
    },{
        "campo": "stableDebtTokenAddress",
        "tipo": "address",
        "description": "stableDebtToken address"
    },{
        "campo": "variableDebtTokenAddress",
        "tipo": "address", 
        "description": "variableDebtToken address"
    },{
        "campo": "interestRateStrategyAddress",
        "tipo": "address",
        "description": "address of the interest rate strategy"
    }
]