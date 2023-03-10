package com.lukasnt.spark.models

import com.lukasnt.spark.models.Types.PathQuery

class VariableQuery(val constantPathQuery: ConstQuery = null,
                    val aggFunc: QueryAggFunc = null,
                    val minLength: Int = 0,
                    val maxLength: Int = 0)
    extends PathQuery {}
