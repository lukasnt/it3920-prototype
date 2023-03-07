package com.lukasnt.spark.models

class ArbitraryPathQuery(val constPathQuery: ConstPathQuery = null,
                         val aggFunc: PathAggFunc = null,
                         val minLength: Int = 0) {}
