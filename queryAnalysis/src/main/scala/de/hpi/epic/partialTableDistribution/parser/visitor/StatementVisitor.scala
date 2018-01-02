package de.hpi.epic.partialTableDistribution.parser.visitor

import de.hpi.epic.partialTableDistribution.utils
import de.hpi.epic.partialTableDistribution.data
import net.sf.jsqlparser.expression.operators.arithmetic._
import net.sf.jsqlparser.expression.operators.conditional.{AndExpression, OrExpression}
import net.sf.jsqlparser.expression._
import net.sf.jsqlparser.expression.operators.relational._
import net.sf.jsqlparser.schema.{Column, Table}
import net.sf.jsqlparser.{expression, statement}
import net.sf.jsqlparser.statement.alter.Alter
import net.sf.jsqlparser.statement.create.index.CreateIndex
import net.sf.jsqlparser.statement.{Commit, SetStatement, Statements}
import net.sf.jsqlparser.statement.create.table.CreateTable
import net.sf.jsqlparser.statement.create.view.{AlterView, CreateView}
import net.sf.jsqlparser.statement.delete.Delete
import net.sf.jsqlparser.statement.drop.Drop
import net.sf.jsqlparser.statement.execute.Execute
import net.sf.jsqlparser.statement.insert.Insert
import net.sf.jsqlparser.statement.merge.Merge
import net.sf.jsqlparser.statement.replace.Replace
import net.sf.jsqlparser.statement.select._
import net.sf.jsqlparser.statement.truncate.Truncate
import net.sf.jsqlparser.statement.update.Update
import net.sf.jsqlparser.statement.upsert.Upsert

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by Jan on 13.11.2017.
  */
class StatementVisitor(tables: Seq[data.Table]) extends statement.StatementVisitor
  with statement.select.SelectVisitor
  with statement.select.SelectItemVisitor
  with ExpressionVisitor with OrderByVisitor with FromItemVisitor with ItemsListVisitor {
  var curId: Int = 0
  val columnMap = mutable.Map.empty[Int, Set[data.Column]]

  override def visit(update: Update): Unit = ???

  override def visit(delete: Delete): Unit = ???

  override def visit(commit: Commit): Unit = ???

  override def visit(execute: Execute): Unit = ???

  override def visit(set: SetStatement): Unit = ???

  override def visit(createIndex: CreateIndex): Unit = ???

  override def visit(truncate: Truncate): Unit = ???

  override def visit(drop: Drop): Unit = ???

  override def visit(replace: Replace): Unit = ???

  override def visit(insert: Insert): Unit = ???

  override def visit(stmts: Statements): Unit = {
    if (stmts.getStatements != null)
      stmts.getStatements.foreach(stmt => {
        curId += 1
        stmt.accept(this)
      })
  }

  override def visit(alter: Alter): Unit = ???

  override def visit(alterView: AlterView): Unit = ???

  override def visit(createView: CreateView): Unit = ???

  override def visit(createTable: CreateTable): Unit = ???

  override def visit(upsert: Upsert): Unit = ???

  override def visit(select: Select): Unit = {
    if (select.getWithItemsList != null)
      select.getWithItemsList.foreach(_.accept(this))
    if (select.getSelectBody != null)
      select.getSelectBody.accept(this)
  }

  override def visit(merge: Merge): Unit = ???

  override def visit(plainSelect: PlainSelect): Unit = {
    if (plainSelect.getSelectItems != null)
      plainSelect.getSelectItems.foreach(_.accept(this))
    if (plainSelect.getFromItem != null)
      plainSelect.getFromItem.accept(this)
    if (plainSelect.getGroupByColumnReferences != null)
      plainSelect.getGroupByColumnReferences.foreach(_.accept(this))
    if (plainSelect.getHaving != null)
      plainSelect.getHaving.accept(this)
    if (plainSelect.getJoins != null)
      plainSelect.getJoins.foreach(join => {
        if (join.getOnExpression != null)
          join.getOnExpression.accept(this)
        if (join.getRightItem != null)
          join.getRightItem.accept(this)
      })
    if (plainSelect.getOrderByElements != null)
      plainSelect.getOrderByElements.foreach(_.accept(this))
    if (plainSelect.getWhere != null)
      plainSelect.getWhere.accept(this)
  }

  override def visit(setOpList: SetOperationList): Unit = {
    if (setOpList.getSelects != null)
      setOpList.getSelects.foreach(_.accept(this))
    if (setOpList.getOrderByElements != null)
      setOpList.getOrderByElements.foreach(_.accept(this))
  }

  override def visit(withItem: WithItem): Unit = {
    if (withItem.getSelectBody != null)
      withItem.getSelectBody.accept(this)
    if (withItem.getWithItemList != null)
      withItem.getWithItemList.foreach(_.accept(this))
  }

  //TODO
  override def visit(allColumns: AllColumns): Unit = ()

  override def visit(allTableColumns: AllTableColumns): Unit = println(allTableColumns.getTable.getName)

  override def visit(selectExpressionItem: SelectExpressionItem): Unit = selectExpressionItem.getExpression.accept(this)

  override def visit(aThis: NotExpression): Unit = aThis.getExpression.accept(this)

  override def visit(inExpression: InExpression): Unit = {
    if (inExpression.getLeftExpression != null)
      inExpression.getLeftExpression.accept(this)
    if (inExpression.getLeftItemsList != null)
      inExpression.getLeftItemsList.accept(this)
    if (inExpression.getRightItemsList != null)
      inExpression.getRightItemsList.accept(this)
  }

  override def visit(greaterThanEquals: GreaterThanEquals): Unit = {
    if (greaterThanEquals.getLeftExpression != null)
      greaterThanEquals.getLeftExpression.accept(this)
    if (greaterThanEquals.getRightExpression != null)
      greaterThanEquals.getRightExpression.accept(this)
  }

  override def visit(greaterThan: GreaterThan): Unit = {
    if (greaterThan.getLeftExpression != null)
      greaterThan.getLeftExpression.accept(this)
    if (greaterThan.getRightExpression != null)
      greaterThan.getRightExpression.accept(this)
  }

  override def visit(equalsTo: EqualsTo): Unit = {
    if (equalsTo.getLeftExpression != null)
      equalsTo.getLeftExpression.accept(this)
    if (equalsTo.getRightExpression != null)
      equalsTo.getRightExpression.accept(this)
  }

  override def visit(between: Between): Unit = {
    if (between.getLeftExpression != null)
      between.getLeftExpression.accept(this)
    if (between.getBetweenExpressionStart != null)
      between.getBetweenExpressionStart.accept(this)
    if (between.getBetweenExpressionEnd != null)
      between.getBetweenExpressionEnd.accept(this)
  }

  override def visit(orExpression: OrExpression): Unit = {
    if (orExpression.getLeftExpression != null)
      orExpression.getLeftExpression.accept(this)
    if (orExpression.getRightExpression != null)
      orExpression.getRightExpression.accept(this)
  }

  override def visit(andExpression: AndExpression): Unit = {
    if (andExpression.getLeftExpression != null)
      andExpression.getLeftExpression.accept(this)
    if (andExpression.getRightExpression != null)
      andExpression.getRightExpression.accept(this)
  }

  override def visit(subtraction: Subtraction): Unit = {
    if (subtraction.getLeftExpression != null)
      subtraction.getLeftExpression.accept(this)
    if (subtraction.getRightExpression != null)
      subtraction.getRightExpression.accept(this)
  }

  override def visit(multiplication: Multiplication): Unit = {
    if (multiplication.getLeftExpression != null)
      multiplication.getLeftExpression.accept(this)
    if (multiplication.getRightExpression != null)
      multiplication.getRightExpression.accept(this)
  }

  override def visit(division: Division): Unit = {
    if (division.getLeftExpression != null)
      division.getLeftExpression.accept(this)
    if (division.getRightExpression != null)
      division.getRightExpression.accept(this)
  }

  override def visit(addition: Addition): Unit = {
    if (addition.getLeftExpression != null)
      addition.getLeftExpression.accept(this)
    if (addition.getRightExpression != null)
      addition.getRightExpression.accept(this)
  }

  override def visit(nullValue: NullValue): Unit = ()

  override def visit(matches: Matches): Unit = ???

  override def visit(bitwiseAnd: BitwiseAnd): Unit = ???

  override def visit(bitwiseOr: BitwiseOr): Unit = ???

  override def visit(bitwiseXor: BitwiseXor): Unit = ???

  override def visit(cast: CastExpression): Unit = {
    if (cast.getLeftExpression != null)
      cast.getLeftExpression.accept(this)
  }

  override def visit(modulo: Modulo): Unit = ???

  override def visit(aexpr: AnalyticExpression): Unit = {
    if (aexpr.getDefaultValue != null)
      aexpr.getDefaultValue.accept(this)
    if (aexpr.getExpression != null)
      aexpr.getExpression.accept(this)
    if (aexpr.getOffset != null)
      aexpr.getOffset.accept(this)
    if (aexpr.getOrderByElements != null)
      aexpr.getOrderByElements.foreach(_.accept(this))
    if (aexpr.getPartitionExpressionList != null)
      aexpr.getPartitionExpressionList.accept(this)
    if (aexpr.getWindowElement != null && aexpr.getWindowElement.getOffset != null &&
      aexpr.getWindowElement.getOffset.getExpression != null)
      aexpr.getWindowElement.getOffset.getExpression.accept(this)
    if (aexpr.getWindowElement != null && aexpr.getWindowElement.getRange != null) {
      if (aexpr.getWindowElement.getRange.getStart != null && aexpr.getWindowElement.getRange.getStart.getExpression != null)
        aexpr.getWindowElement.getRange.getStart.getExpression.accept(this)
      if (aexpr.getWindowElement.getRange.getEnd != null && aexpr.getWindowElement.getRange.getEnd.getExpression != null)
        aexpr.getWindowElement.getRange.getEnd.getExpression.accept(this)
    }
  }

  override def visit(wgexpr: WithinGroupExpression): Unit = ???

  override def visit(eexpr: ExtractExpression): Unit = eexpr.getExpression.accept(this)

  override def visit(iexpr: IntervalExpression): Unit = ()

  override def visit(oexpr: OracleHierarchicalExpression): Unit = ???

  override def visit(isNullExpression: IsNullExpression): Unit =
    if (isNullExpression.getLeftExpression != null) isNullExpression.getLeftExpression.accept(this)

  override def visit(function: expression.Function): Unit = {
    if (function.getParameters != null && function.getParameters.getExpressions != null)
      function.getParameters.getExpressions.foreach(_.accept(this))
  }

  override def visit(signedExpression: SignedExpression): Unit =
    if (signedExpression.getExpression != null) signedExpression.getExpression.accept(this)

  override def visit(jdbcParameter: JdbcParameter): Unit = ()

  override def visit(jdbcNamedParameter: JdbcNamedParameter): Unit = println(jdbcNamedParameter.getName)

  override def visit(doubleValue: DoubleValue): Unit = ()

  override def visit(longValue: LongValue): Unit = ()

  override def visit(hexValue: HexValue): Unit = ()

  override def visit(dateValue: DateValue): Unit = ()

  override def visit(timeValue: TimeValue): Unit = ()

  override def visit(timestampValue: TimestampValue): Unit = ()

  override def visit(parenthesis: Parenthesis): Unit = if (parenthesis.getExpression != null) parenthesis.getExpression.accept(this)

  override def visit(concat: Concat): Unit = {
    concat.getLeftExpression.accept(this)
    concat.getRightExpression.accept(this)
  }

  override def visit(anyComparisonExpression: AnyComparisonExpression): Unit = {
    if (anyComparisonExpression.getSubSelect != null)
      anyComparisonExpression.getSubSelect.getSelectBody.accept(this)
  }

  override def visit(allComparisonExpression: AllComparisonExpression): Unit = {
    if (allComparisonExpression.getSubSelect != null)
      allComparisonExpression.getSubSelect.getSelectBody.accept(this)
  }

  override def visit(existsExpression: ExistsExpression): Unit = {
    if (existsExpression.getRightExpression != null)
      existsExpression.getRightExpression.accept(this)
  }

  override def visit(whenClause: WhenClause): Unit = {
    if (whenClause.getWhenExpression != null)
      whenClause.getWhenExpression.accept(this)
    if (whenClause.getThenExpression != null)
      whenClause.getThenExpression.accept(this)
  }

  override def visit(caseExpression: CaseExpression): Unit = {
    if (caseExpression.getSwitchExpression != null)
      caseExpression.getSwitchExpression.accept(this)
    if (caseExpression.getWhenClauses != null)
      caseExpression.getWhenClauses.foreach(_.accept(this))
    if (caseExpression.getElseExpression != null)
      caseExpression.getElseExpression.accept(this)
  }

  override def visit(subSelect: SubSelect): Unit = {
    if (subSelect.getSelectBody != null)
      subSelect.getSelectBody.accept(this)
    if (subSelect.getWithItemsList != null)
      subSelect.getWithItemsList.foreach(_.accept(this))
  }

  override def visit(tableColumn: Column): Unit = {
    tables.flatMap(_.columns.find(_.name.toLowerCase == tableColumn.getColumnName.toLowerCase)).headOption.foreach(col => {
      columnMap.update(curId, columnMap.getOrElse(curId, Set.empty) + col)
    })
  }

  override def visit(notEqualsTo: NotEqualsTo): Unit =  {
    if (notEqualsTo.getLeftExpression != null)
      notEqualsTo.getLeftExpression.accept(this)
    if (notEqualsTo.getRightExpression != null)
      notEqualsTo.getRightExpression.accept(this)
  }

  override def visit(minorThanEquals: MinorThanEquals): Unit =  {
    if (minorThanEquals.getLeftExpression != null)
      minorThanEquals.getLeftExpression.accept(this)
    if (minorThanEquals.getRightExpression != null)
      minorThanEquals.getRightExpression.accept(this)
  }

  override def visit(minorThan: MinorThan): Unit =  {
    if (minorThan.getLeftExpression != null)
      minorThan.getLeftExpression.accept(this)
    if (minorThan.getRightExpression != null)
      minorThan.getRightExpression.accept(this)
  }

  override def visit(likeExpression: LikeExpression): Unit = {
    if (likeExpression.getLeftExpression != null)
      likeExpression.getLeftExpression.accept(this)
    if (likeExpression.getRightExpression != null)
      likeExpression.getRightExpression.accept(this)
  }

  override def visit(stringValue: StringValue): Unit = ()

  override def visit(jsonExpr: JsonExpression): Unit = ???

  override def visit(jsonExpr: JsonOperator): Unit = ???

  override def visit(regExpMySQLOperator: RegExpMySQLOperator): Unit = ???

  override def visit(`var`: UserVariable): Unit = ???

  override def visit(bind: NumericBind): Unit = ???

  override def visit(aexpr: KeepExpression): Unit = ???

  override def visit(groupConcat: MySQLGroupConcat): Unit = ???

  override def visit(rowConstructor: RowConstructor): Unit = ???

  override def visit(hint: OracleHint): Unit = ???

  override def visit(timeKeyExpression: TimeKeyExpression): Unit = ???

  override def visit(literal: DateTimeLiteralExpression): Unit = ()

  override def visit(rexpr: RegExpMatchOperator): Unit = ???

  override def visit(orderBy: OrderByElement): Unit = {
    if (orderBy.getExpression != null)
      orderBy.getExpression.accept(this)
  }

  override def visit(tableName: Table): Unit = ()

  override def visit(subjoin: SubJoin): Unit = {
    if (subjoin.getJoin != null) {
      val join = subjoin.getJoin
      if (join.getOnExpression != null)
        join.getOnExpression.accept(this)
      if (join.getRightItem != null)
        join.getRightItem.accept(this)
    }
    if (subjoin.getLeft != null)
      subjoin.getLeft.accept(this)
  }

  override def visit(lateralSubSelect: LateralSubSelect): Unit = {
    if (lateralSubSelect.getSubSelect != null)
      lateralSubSelect.getSubSelect.getSelectBody.accept(this)
  }

  override def visit(valuesList: ValuesList): Unit = {
    if (valuesList.getMultiExpressionList != null)
      valuesList.getMultiExpressionList.accept(this)
  }

  override def visit(tableFunction: TableFunction): Unit = {
    if (tableFunction.getFunction != null)
      tableFunction.getFunction.accept(this)
  }

  override def visit(expressionList: ExpressionList): Unit = {
    if (expressionList.getExpressions != null)
      expressionList.getExpressions.foreach(_.accept(this))
  }

  override def visit(multiExprList: MultiExpressionList): Unit = {
    if (multiExprList.getExprList != null)
      multiExprList.getExprList.foreach(_.accept(this))
  }
}
