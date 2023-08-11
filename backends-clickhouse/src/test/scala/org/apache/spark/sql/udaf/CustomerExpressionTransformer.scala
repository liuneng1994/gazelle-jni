/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.udaf

import io.glutenproject.expression._
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.extension.ExpressionExtensionTrait
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}

import com.google.common.collect.Lists

class KeBitmapFunctionTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Expression)
  extends ExpressionTransformer
  with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.doTransform(args)
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitExprName,
        original.children.map(_.dataType),
        FunctionConfig.OPT))

    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

case class CustomerExpressionTransformer() extends ExpressionExtensionTrait {

  /** Generate the extension expressions list, format: Sig[XXXExpression]("XXXExpressionName") */
  def expressionSigList: Seq[Sig] = Seq(
    Sig[PreciseCardinality]("ke_bitmap_cardinality"),
    Sig[PreciseCountDistinctDecode]("ke_bitmap_cardinality"),
    Sig[ReusePreciseCountDistinct]("ke_bitmap_or"),
    Sig[PreciseCountDistinctAndValue]("ke_bitmap_and_value"),
    Sig[PreciseCountDistinctAndArray]("ke_bitmap_and_ids"),
    Sig[PreciseCountDistinct]("ke_bitmap_or_cardinality")
  )

  /** Replace extension expression to transformer. */
  override def replaceWithExtensionExpressionTransformer(
      substraitExprName: String,
      expr: Expression,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = expr match {
    case preciseCardinality: PreciseCardinality =>
      new KeBitmapFunctionTransformer(
        substraitExprName,
        ExpressionConverter
          .replaceWithExpressionTransformer(preciseCardinality.child, attributeSeq),
        preciseCardinality
      )
    case preciseCountDistinctDecode: PreciseCountDistinctDecode =>
      new KeBitmapFunctionTransformer(
        substraitExprName,
        ExpressionConverter
          .replaceWithExpressionTransformer(preciseCountDistinctDecode.child, attributeSeq),
        preciseCountDistinctDecode
      )
    case other =>
      throw new UnsupportedOperationException(
        s"${expr.getClass} or $expr is not currently supported.")
  }
}
