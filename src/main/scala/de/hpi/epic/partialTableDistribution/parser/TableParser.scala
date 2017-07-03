package de.hpi.epic.partialTableDistribution.parser

import de.hpi.epic.partialTableDistribution.utils.{Column, SQLVarChar, Table}

import scala.util.parsing.combinator._
import scala.util.parsing.combinator.syntactical._

object TableParser extends RegexParsers {
  private val eol = "\n" | "\r\n" //sys.props("line.separator")
  private val eoi = """\z""".r
  private val separator = eoi | eol
  override val skipWhitespace = false

  def tableList: Parser[Seq[Table]] = repsep(table, eol)
  def table: Parser[Table] = identifier ~ (": " ~> integer <~ " rows") ~ (eol ~> columnList) ^^ {case name ~ rows ~ columns => Table(name, columns.map(_.copy(rows = rows)))}
  def columnList: Parser[Seq[Column]] = rep(column <~ separator)
  def column: Parser[Column] = identifier ~ ("," ~> integer) ^^ {case name ~ size => Column(name, SQLVarChar, Some(size))}

  def identifier: Parser[String] = "[a-zA-Z_][_a-zA-Z0-9_]*".r
  def integer: Parser[Int] = "[0-9]+".r ^^ {str => str.toInt}

  def parse(input: String) = parseAll(tableList, input) match {
    case Success(res, _) => res
    case res => throw new Exception(res.toString)
  }
}