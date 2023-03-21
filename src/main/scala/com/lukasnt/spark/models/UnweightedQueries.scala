package com.lukasnt.spark.models

import com.lukasnt.spark.models.Types.PathQuery

class UnweightedQueries(sequence: List[(PathQuery, QueryAggFunc)]) extends SequencedQueries(sequence) {}
