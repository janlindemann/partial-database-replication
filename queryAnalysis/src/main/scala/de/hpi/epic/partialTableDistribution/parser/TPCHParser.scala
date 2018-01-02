package de.hpi.epic.partialTableDistribution.parser

import de.hpi.epic.partialTableDistribution.sql.Query

/**
  * Created by Jan on 27.05.2017.
  */
object TPCHParser extends SQLParser {
  def queryId: Parser[Int] = "Query" ~> integer
  def queryList: Parser[Seq[(Int, Query)]] = repsep(queryId ~ query ^^ { case id ~ query => (id, query)}, ";")
  def parseAll(input: String): Seq[(Int, Query)] = parseAll(queryList, input) match {
    case Success(res, _) => res
    case res => throw new Exception(res.toString)
  }
}
