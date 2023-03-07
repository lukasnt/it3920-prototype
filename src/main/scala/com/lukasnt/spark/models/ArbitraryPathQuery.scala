package com.lukasnt.spark.models

import com.lukasnt.spark.models.Types.PathQuery

class ArbitraryPathQuery(val constPathQuery: ConstPathQuery = null,
                         val aggFunc: PathAggFunc = null,
                         val minLength: Int = 0) extends PathQuery {}
