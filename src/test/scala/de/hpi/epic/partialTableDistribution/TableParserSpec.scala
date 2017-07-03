package de.hpi.epic.partialTableDistribution

import de.hpi.epic.partialTableDistribution.parser.TableParser
import de.hpi.epic.partialTableDistribution.utils.{Column, SQLVarChar, Table}
import org.scalatest.{MustMatchers, WordSpec}

/**
  * Created by Jan on 24.05.2017.
  */
class TableParserSpec extends WordSpec with MustMatchers {
  val tableDef = """PART: 0 rows
                   |P_PARTKEY,4
                   |P_NAME,56""".stripMargin

  val tableDef2 = """PART: 0 rows
                    |P_PARTKEY,4
                    |P_NAME,56
                    |
                    |ORDER: 0 rows
                    |O_ORDERKEY,12
                    |O_AMOUNT,11""".stripMargin

  "The parser" should {
    "parse one table" in {
      val res = TableParser.parse(tableDef)
      res.length mustBe 1
      val table = res.head
      table.name mustBe "PART"
      table.columns mustEqual Seq(Column("P_PARTKEY", SQLVarChar, Some(4)), Column("P_NAME", SQLVarChar, Some(56)))
    }

    "parse multiple tables" in {
      val res = TableParser.parse(tableDef2)
      res.length mustBe 2
      res mustEqual Seq(
        Table("PART", Seq(Column("P_PARTKEY", SQLVarChar, Some(4)), Column("P_NAME", SQLVarChar, Some(56)))),
        Table("ORDER", Seq(Column("O_ORDERKEY", SQLVarChar, Some(12)), Column("O_AMOUNT", SQLVarChar, Some(11))))
      )
    }
  }
}
