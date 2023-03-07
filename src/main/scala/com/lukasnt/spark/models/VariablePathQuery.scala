package com.lukasnt.spark.models

import com.lukasnt.spark.models.Types.PathQuery

class VariablePathQuery(val constantPathQuery: ConstPathQuery = null,
                        val aggFunc: PathAggFunc = null,
                        val minLength: Int = 0,
                        val maxLength: Int = 0)
    extends PathQuery {}
