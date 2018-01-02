package de.hpi.epic.partialTableDistribution.parser.visitor

import de.hpi.epic.partialTableDistribution.data.{Column, Table}
import de.hpi.epic.partialTableDistribution.utils.CSVFile
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement
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
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.statement.truncate.Truncate
import net.sf.jsqlparser.statement.update.Update
import net.sf.jsqlparser.statement.upsert.Upsert

import collection.mutable
import collection.JavaConversions._
import scala.io.Source

/**
  * Created by Jan on 15.11.2017.
  */
class TableMetadataExtractor(tableSizes: CSVFile) extends statement.StatementVisitor {
  private[visitor] val tableBuffer = mutable.ListBuffer.empty[Table]

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
      stmts.getStatements.foreach(_.accept(this))
  }

  override def visit(alter: Alter): Unit = ???

  override def visit(alterView: AlterView): Unit = ???

  override def visit(createView: CreateView): Unit = ???

  override def visit(createTable: CreateTable): Unit = {
    if (createTable.getTable != null && createTable.getColumnDefinitions != null) {
      val tableName = createTable.getTable.getName
      val row_count = tableSizes.find(_.getString("table").toLowerCase == tableName.toLowerCase).map(_.getInt("rows")).getOrElse(0)
      val columns = createTable.getColumnDefinitions.map(col => {
        val length = col.getColDataType.getDataType.toLowerCase match {
          case "serial" => 4
          case "integer" => 4
          case "date" => 8
          case "time" => 8
          case "decimal" => 8
          case "char" => col.getColDataType.getArgumentsStringList.head.toInt
          case "varchar" => col.getColDataType.getArgumentsStringList.head.toInt
        }
        Column(col.getColumnName, length, row_count)
      }).toList
      tableBuffer += Table(tableName, columns)
    }
  }

  override def visit(upsert: Upsert): Unit = ???

  override def visit(select: Select): Unit = ???

  override def visit(merge: Merge): Unit = ???
}

object TableMetadataExtractor {
  def extractTables(tableDefFilePath: String, tableSizeFilePath: String): Seq[Table] = {
    val tableSizes = CSVFile(tableSizeFilePath)
    val source = Source.fromFile(tableDefFilePath).getLines().mkString(System.lineSeparator())
    val stmts = CCJSqlParserUtil.parseStatements(source)
    val extractor = new TableMetadataExtractor(tableSizes)
    stmts.accept(extractor)
    extractor.tableBuffer.toList
  }
}