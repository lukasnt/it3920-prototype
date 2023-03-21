package com.lukasnt.spark.models

import com.lukasnt.spark.models.Types.PathQuery

class WeightedQueries(sequence: List[(PathQuery, QueryAggFunc)]) extends SequencedQueries(sequence) {}
