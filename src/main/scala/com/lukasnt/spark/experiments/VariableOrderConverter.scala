package com.lukasnt.spark.experiments

import picocli.CommandLine.ITypeConverter

class VariableOrderConverter extends ITypeConverter[Experiment.VariableOrder.Value] {

  override def convert(s: String): Experiment.VariableOrder.Value = {
    s.toLowerCase() match {
      case "ascending"  => Experiment.VariableOrder.Ascending
      case "asc"        => Experiment.VariableOrder.Ascending
      case "descending" => Experiment.VariableOrder.Descending
      case "desc"       => Experiment.VariableOrder.Descending
      case "shuffled"   => Experiment.VariableOrder.Shuffled
      case "shuffle"    => Experiment.VariableOrder.Shuffled
      case _            => Experiment.VariableOrder.Ascending
    }
  }

}
