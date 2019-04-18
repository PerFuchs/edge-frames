package sparkIntegration

import leapfrogTriejoin.TreeTrieIterator
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.slf4j.LoggerFactory

import Predef._

case class WCOJ(joinSpecification: JoinSpecification, children: Seq[LogicalPlan]) extends LogicalPlan {
  private val logger = LoggerFactory.getLogger(classOf[WCOJ])

  logger.error(children.map(c => c.output.map(_.name).mkString(",")).mkString(";"))

  // TODO if I cache the input dataset there is one stage in query planning in which the output is unnamed (none). Find a different place to require this.
//  require(joinSpecification.edges.size == children.size, "WCOJ needs as many relationship as edges in the JoinSpecification")
//  require(children.forall(c => c.output.size == 2 && List("src", "dst").forall(s => c.output.map(_.name).contains(s))), "Each children of a WCOJ should have two attributes called src and dst") // TODO handle general case of more attributes.

  override def references: AttributeSet = AttributeSet(children.flatMap(c => c.output.filter(a => List("src", "dst").contains(a.name))))

  override def output: Seq[Attribute] = {
    joinSpecification.allVariables.map(v => {
      val relI = joinSpecification.variableToRelationshipIndex(v)
      val aI = joinSpecification.variableToAttributeIndex(v)
      val ref = children(relI).output.filter(a => a.name == (if (aI == 0) "src" else "dst")).head
      ref.withName(v)
    })
  }
}
