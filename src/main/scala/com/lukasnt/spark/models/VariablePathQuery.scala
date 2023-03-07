package com.lukasnt.spark.models

class VariablePathQuery(val constantPathQuery: ConstPathQuery = null,
                        val aggFunc: PathAggFunc = null,
                        val minLength: Int = 0,
                        val maxLength: Int = 0) {}
