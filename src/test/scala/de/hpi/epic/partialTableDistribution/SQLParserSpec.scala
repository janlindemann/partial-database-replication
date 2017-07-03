package de.hpi.epic.partialTableDistribution

import de.hpi.epic.partialTableDistribution.parser.SQLParser
import de.hpi.epic.partialTableDistribution.sql._
import org.scalatest.{MustMatchers, WordSpec}

/**
  * Created by Jan on 25.05.2017.
  */
class SQLParserSpec extends WordSpec with MustMatchers {
  "The parser" should {
    "parse a single selected column" in {
      SQLParser.parse("select\nl_returnflag\nfrom\nlineitem") mustEqual
        Select(
          Seq(AliasExpression(Column("l_returnflag"))),
          Seq(AliasCorrelation(Table("lineitem")))
        )
    }

    "parse multiple selected columns" in {
      SQLParser.parse("select\nl_returnflag,\nl_linestatus\nfrom\nlineitem") mustEqual
        Select(
          Seq(AliasExpression(Column("l_returnflag")), AliasExpression(Column("l_linestatus"))),
          Seq(AliasCorrelation(Table("lineitem")))
        )
    }

    "parse sum aggregations" in {
      SQLParser.parse("select\nl_returnflag,\nl_linestatus,\nsum(l_quantity) as sum_qty\nfrom\nlineitem") mustEqual
        Select(
          Seq(
            AliasExpression(Column("l_returnflag")),
            AliasExpression(Column("l_linestatus")),
            AliasExpression(Sum(Column("l_quantity")), Some("sum_qty"))
          ),
          Seq(AliasCorrelation(Table("lineitem"))))
    }

    "parse constants and operations" in {
      SQLParser.parse("select sum(l_extendedprice*(1-l_discount)) as sum_disc_price\nfrom\nlineitem") mustEqual
        Select(
          Seq(
            AliasExpression(Sum(
              OperationExpression(
                Column("l_extendedprice"),
                "*",
                OperationExpression(
                  SQLInteger(1),
                  "-",
                  Column("l_discount")
                ))),
              Some("sum_disc_price")
            )
          ),
          Seq(AliasCorrelation(Table("lineitem")))
        )
    }

    "parse a group by clause" in {
      SQLParser.parse("select\nl_returnflag,\nl_linestatus,\nsum(l_quantity) as sum_qty\nfrom\nlineitem\ngroup by l_returnflag,\nl_linestatus") mustEqual
        Select(
          Seq(
            AliasExpression(Column("l_returnflag")),
            AliasExpression(Column("l_linestatus")),
            AliasExpression(Sum(Column("l_quantity")), Some("sum_qty"))
          ),
          Seq(AliasCorrelation(Table("lineitem"))),
          grouping = Some(Seq(Column("l_returnflag"), Column("l_linestatus")))
        )
    }

    "parse a condition" in {
      SQLParser.parse("select\nl_returnflag\nfrom\nlineitem\nwhere\nl_returnflag = l_returnflag") mustEqual
        Select(
          Seq(AliasExpression(Column("l_returnflag"))),
          Seq(AliasCorrelation(Table("lineitem"))),
          Some(Comparison(Column("l_returnflag"), Equal, None, Left(Seq(Column("l_returnflag")))))
        )
    }

    "parse a whole query" in {
      val res = SQLParser.parse("select\nl_returnflag,\nl_linestatus,\nsum(l_quantity) as sum_qty,\nsum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty,\navg(l_extendedprice) as avg_price,\navg(l_discount) as avg_disc,\ncount(*) as count_order\nfrom\nlineitem\nwhere\nl_shipdate <= date '1998-12-01' - interval '[DELTA]' day (3)\ngroup by l_returnflag,\nl_linestatus order by\nl_returnflag, l_linestatus")
      def getColumns(p: Seq[String], c: SQLElement): Seq[String] = {
        c match {
          case Column(name, _) => name +: p
          case _ => p
        }
      }
      Extractor.fold(Seq.empty[String], getColumns, res).distinct.sorted mustEqual
        Seq("l_discount", "l_extendedprice", "l_linestatus", "l_quantity", "l_returnflag", "l_shipdate", "l_tax")
    }

    "parse this query" in {
      val res = SQLParser.parse("select s_suppkey, s_name, s_address, s_phone, total_revenue\nfrom supplier, (\n  select l_suppkey, sum(l_extendedprice * (1 - l_discount)) \n  from lineitem \n  where l_shipdate >= date '[DATE]' and l_shipdate < date '[DATE]' + interval '3' month\n  group by l_suppkey\n)\nwhere s_suppkey = supplier_no and total_revenue = (\n  select max(total_revenue)\n  from (\n    select l_suppkey, sum(l_extendedprice * (1 - l_discount)) \n    from lineitem \n    where l_shipdate >= date '[DATE]' and l_shipdate < date '[DATE]' + interval '3' month\n    group by l_suppkey\n  )\n) order by s_suppkey")
      def getColumns(p: Seq[String], c: SQLElement): Seq[String] = {
        c match {
          case Column(name, _) => name +: p
          case _ => p
        }
      }
      Extractor.fold(Seq.empty[String], getColumns, res).distinct.sorted mustEqual
        Seq("l_extendedprice", "l_discount", "l_shipdate", "l_quantity").distinct.sorted
    }
  }
}
