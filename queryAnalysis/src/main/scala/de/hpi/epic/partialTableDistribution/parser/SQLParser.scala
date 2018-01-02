package de.hpi.epic.partialTableDistribution.parser

import de.hpi.epic.partialTableDistribution.sql._

import scala.util.parsing.combinator.RegexParsers

trait SQLParser extends RegexParsers {
  class MyRichString(str: String) {
    def ignoreCase: Parser[String] = ("""(?i)\Q""" + str + """\E""").r
  }

  implicit def pimpString(str: String): MyRichString = new MyRichString(str)

  def identifier: Parser[String] = "[a-zA-Z_][_a-zA-Z0-9]*".r
  def integer: Parser[Int] = "[0-9]+".r ^^ {_.toInt}
  def float: Parser[Float] = "[0-9]+\\.[0-9]+".r ^^ {_.toFloat}
  def string: Parser[String] = "'[%a-zA-Z0-9\\-_.*+/\\\\\\[\\]\\s]+'".r
  def variable: Parser[SQLVariable] = ("[" ~> "[a-zA-Z0-9_]+".r <~ "]") ^^ {SQLVariable(_)}

  def constant: Parser[Constant] = variable | (float ^^ {SQLFloat(_)}) | (integer ^^ {SQLInteger(_)}) | (string ^^ {SQLString(_)})

  def as: Parser[String] = "as".ignoreCase
  def asc: Parser[SQLOrdering] = "asc".ignoreCase ^^ { _ => ASC }
  def desc: Parser[SQLOrdering] = "desc".ignoreCase ^^ { _ => DESC }
  def count: Parser[String] = "count".ignoreCase ^^ { _.toLowerCase }
  def sum: Parser[String] = "sum".ignoreCase ^^ { _.toLowerCase }
  def avg: Parser[String] = "avg".ignoreCase ^^ { _.toLowerCase }
  def select: Parser[String] = "select".ignoreCase
  def from: Parser[String] = "from".ignoreCase
  def group: Parser[String] = "group".ignoreCase
  def order: Parser[String] = "order".ignoreCase
  def by: Parser[String] = "by".ignoreCase
  def where: Parser[String] = "where".ignoreCase
  def like: Parser[String] = "like".ignoreCase
  def or: Parser[String] = "or".ignoreCase
  def and: Parser[String] = "and".ignoreCase
  def not:Parser[String] = "not".ignoreCase
  def any: Parser[ComparisonQualifier] = "any".ignoreCase ^^^ ANY
  def some: Parser[ComparisonQualifier] = "some".ignoreCase ^^^ ANY
  def all: Parser[ComparisonQualifier] = "all".ignoreCase ^^^ ALL
  def equal: Parser[Comparator] =  "=" ^^^ Equal
  def notEqual: Parser[Comparator] =  ("!=" | "<>") ^^^ NotEqual
  def greaterEqual: Parser[Comparator] =  ">=" ^^^ GreaterEqual
  def greaterThan: Parser[Comparator] =  ">" ^^^ GreaterThan
  def lessEqual: Parser[Comparator] =  "<=" ^^^ LessEqual
  def lessThan: Parser[Comparator] =  "<" ^^^ LessThan
  def date: Parser[String] = "date".ignoreCase
  def interval: Parser[String] = "interval".ignoreCase
  def year: Parser[DatetimeField] = "YEAR".ignoreCase ^^^ YEAR
  def month: Parser[DatetimeField] = "MONTH".ignoreCase ^^^ MONTH
  def day: Parser[DatetimeField] = "DAY".ignoreCase ^^^ DAY
  def hour: Parser[DatetimeField] = "HOUR".ignoreCase ^^^ HOUR
  def minute: Parser[DatetimeField] = "MINUTE".ignoreCase ^^^ MINUTE
  def min: Parser[String] = "min".ignoreCase ^^ {_.toLowerCase}
  def max: Parser[String] = "max".ignoreCase  ^^ {_.toLowerCase}
  def exists: Parser[String] = "exists".ignoreCase
  def between: Parser[String] = "between".ignoreCase
  def extract: Parser[String] = "extract".ignoreCase
  def case_ : Parser[String] = "case".ignoreCase
  def when_ : Parser[String] = "when".ignoreCase
  def then_ : Parser[String] = "then".ignoreCase
  def else_ : Parser[String] = "else".ignoreCase
  def end: Parser[String] = "end".ignoreCase
  def having: Parser[String] = "having".ignoreCase
  def in: Parser[String] = "in".ignoreCase
  def join: Parser[String] = "join".ignoreCase
  def inner: Parser[JoinType] = "inner".ignoreCase ^^^ INNER
  def outer: Parser[String] = "outer".ignoreCase
  def left_outer: Parser[JoinType] = "left".ignoreCase <~ opt(outer) ^^^ LEFT_OUTER
  def right_outer: Parser[JoinType] = "right".ignoreCase <~ opt(outer) ^^^ RIGHT_OUTER
  def full_outer: Parser[JoinType] = "full".ignoreCase <~ opt(outer) ^^^ FULL_OUTER
  def union_join: Parser[JoinType] = "union".ignoreCase ^^^ UNION
  def on: Parser[String] = "on".ignoreCase
  def natural: Parser[String] = "natural".ignoreCase
  def distinct: Parser[String] = "distinct".ignoreCase
  def substring: Parser[String] = "substring".ignoreCase
  def for_ : Parser[String] = "for".ignoreCase

  def datetimeField: Parser[DatetimeField] = year | month | day | hour | minute

  def column: Parser[Column] = opt(identifier <~ ".") ~ identifier ^^ { case tableName ~ columnName => Column(columnName, tableName)}

  def addExpression: Parser[Expression] = mulExpression ~ opt(("+" | "-") ~ addExpression) ^^ {
    case exp ~ None => exp
    case exp ~ Some(op ~ epx2) => OperationExpression(exp, op, epx2)
  }
  def mulExpression: Parser[Expression] = singleExpression ~ opt(("*" | "/") ~ mulExpression) ^^ {
    case exp ~ None => exp
    case exp ~ Some(op ~ exp2) => OperationExpression(exp, op, exp2)
  }
  def singleExpression: Parser[Expression] = constant | paranthesisExpression | caseExpression | aggregationExpression | functionExpression | column
  def caseExpression: Parser[CaseExpression] = case_ ~> rep1(whenClause) ~ opt(else_ ~> expression) <~ end ^^ { case w ~ e => SearchCaseExpression(w, e)}
  def whenClause: Parser[SearchCase] = when_ ~> condition ~ (then_ ~> expression) ^^ { case c ~ e => SearchCase(c, e)}
  def functionExpression: Parser[SQLFunction] = extractFunction | dateFunction | intervalFunction | substringFunction
  def substringFunction: Parser[SubstringFunction] = substring ~> "(" ~> expression ~ (from ~> integer) ~ opt(for_ ~> integer) <~ ")" ^^ {
    case exp ~ pos ~ length => SubstringFunction(exp, pos, length)
  }
  def dateFunction: Parser[DateFunction] = date ~> expression ^^ {str => DateFunction(str)}
  def extractFunction: Parser[ExtractFunction] = extract ~> "(" ~> datetimeField ~ ( from ~> expression <~ ")") ^^ {
    case datetimeField ~ expression => ExtractFunction(datetimeField, expression)
  }
  def intervalFunction: Parser[IntervalFunction] = interval ~> string ~ datetimeField ~ opt("(" ~> integer <~ ")") ^^ {
    case duration ~ field ~ precision => IntervalFunction(duration, IntervalQualifier(field, precision))
  }
  def aggregationExpression: Parser[AggregationExpression] = countAggregationExpression | simpleAggregationExpression
  def simpleAggregationExpression: Parser[AggregationExpression] = (count | sum | avg | min | max) ~ ( "(" ~> opt(distinct) ~ (expression <~ ")") ) ^^ {
    case "sum" ~ (distinct ~ exp) => Sum(exp, distinct.isDefined)
    case "avg" ~ (distinct ~ exp) => Avg(exp, distinct.isDefined)
    case "min" ~ (distinct ~ exp) => Min(exp, distinct.isDefined)
    case "max" ~ (distinct ~ exp) => Max(exp, distinct.isDefined)
    case "count" ~ (distinct ~ exp) => Count(exp, distinct.isDefined)
  }
  def countAggregationExpression: Parser[AggregationExpression] = count ~ "(" ~ "*" ~ ")" ^^^ CountStar
  def paranthesisExpression: Parser[Expression] = "(" ~> expression <~ ")"
  def expression: Parser[Expression] = addExpression
  def aliasExpression: Parser[AliasExpression] = expression ~ opt(as ~> identifier) ^^ { case exp ~ alias => AliasExpression(exp, alias)}
  def starExpression: Parser[StarExpression.type] = "*" ^^^ StarExpression
  def selectClause: Parser[Seq[Projection]] = select ~> repsep(aliasExpression | starExpression, ",")
  def selectStatement: Parser[Select] = selectClause ~ fromClause ~ opt(whereClause) ~ opt(groupByClause) ~
    opt(havingClause) ~ opt(orderByClause) ^^ {
      case expressions ~ correlations ~ condition ~ grouping ~ having ~ ordering =>
        Select(expressions, correlations, condition, grouping, having, ordering)
    }

  def table: Parser[AliasCorrelation] = opt(identifier <~ ".") ~ identifier ~ opt(as ~> identifier <~ opt("(" ~ rep1sep(identifier, ",")~ ")" )) ^^ {
    case schema ~ name ~ alias => AliasCorrelation(Table(name, schema), alias)
  }
  def subquery: Parser[AliasCorrelation] = ("(" ~> selectStatement <~ ")") ~ opt(as ~> identifier <~ opt("(" ~ rep1sep(identifier, ",")~ ")" )) ^^ {
    case cor ~ alias => AliasCorrelation(cor, alias)
  }
  def table_reference: Parser[AliasCorrelation] = table | subquery
  def join_type: Parser[JoinType] = inner | left_outer | right_outer | full_outer | union_join
  def joined_table: Parser[AliasCorrelation] = table_reference ~  opt(natural) ~ opt(join_type) ~ join ~ correlation ~ opt(on ~> condition) ^^ {
    case left ~ natural ~ join_type ~ _ ~ right ~ condition =>
      AliasCorrelation(Join(left, right, natural.isDefined, join_type.getOrElse(INNER), condition))
  }
  def correlation: Parser[AliasCorrelation] = joined_table | table_reference
  def fromClause: Parser[Seq[AliasCorrelation]] = from ~> rep1sep(correlation, ",")

  def groupByClause: Parser[Seq[Expression]] = group ~> by ~> rep1sep(expression, ",")

  def condition: Parser[Condition] = andCondition ~ opt(or ~> condition) ^^ {
    case cond1 ~ None => cond1
    case cond1 ~ Some(cond2) => DisjunctionCondition(cond1, cond2)
  }
  def andCondition: Parser[Condition] = notCondition ~ opt(and ~> andCondition) ^^ {
    case cond1 ~ None => cond1
    case cond1 ~ Some(cond2) => ConjunctionCondition(cond1, cond2)
  }
  def notCondition: Parser[Condition] = ((not ~> notCondition) ^^ {case cond => NegationCondition(cond)}) | bracketCondition
  def bracketCondition: Parser[Condition] = ("(" ~> (condition | predicate) <~ ")") | predicate
  def predicate: Parser[Predicate] = existsPredicate | inPredicate | betweenPredicate | comparison | likePredicate
  def betweenPredicate: Parser[Between] = expression ~ opt(not) ~ (between ~> expression ~ (and ~> expression)) ^^ {
    case  left ~ negation ~ (start ~ end) => Between(left, start, end, negation.isDefined)
  }
  def existsPredicate: Parser[Exists] = opt(not) ~ (exists ~> "(" ~> selectStatement <~ ")") ^^ {
    case negation ~ query => Exists(query, negation.isDefined)
  }
  def inPredicate: Parser[In] = expression ~ opt(not) ~ (in ~> "(" ~> (selectStatement | rep1sep(expression, ",")) <~ ")") ^^ {
    case left ~ negation ~ (right: Seq[Expression]) => In(left, Left(right), negation.isDefined)
    case left ~ negation ~ (right: Select) => In(left, Right(right), negation.isDefined)
  }
  def likePredicate: Parser[Like] = expression ~ opt(not) ~ (like ~> expression) ^^ {
    case left ~ negation ~ right => Like(left, right, negation.isDefined)
  }
  def comparator: Parser[Comparator] = equal | notEqual | greaterEqual | greaterThan | lessEqual | lessThan
  def comparison: Parser[Comparison] = expression ~ comparator ~ opt(all | any | some) ~ (("(" ~> selectStatement <~ ")") | ("(" ~> rep1sep(expression, ",") <~ ")") | expression) ^^ {
    case left ~ op ~ qualifier ~ (exp: Expression) => Comparison(left, op, qualifier, Left(Seq(exp)))
    case left ~ op ~ qualifier ~ (right: Seq[Expression]) => Comparison(left, op, qualifier, Left(right))
    case left ~ op ~ qualifier ~ (right: Select) => Comparison(left, op, qualifier, Right(right))
  }
  def whereClause: Parser[Condition] = where ~> condition
  def havingClause: Parser[Condition] = having ~> condition

  def orderByExpression: Parser[Order] = expression ~ opt(asc | desc) ^^ {
    case exp ~ None => Order(exp)
    case exp ~ Some(ordering) => Order(exp, ordering)
  }
  def orderByClause: Parser[Seq[Order]] = order ~> by ~> rep1sep(orderByExpression, ",")

  def query: Parser[Query] = selectStatement

  def parse(input: String) = parseAll(query, input) match {
    case Success(res, _) => res
    case res => throw new Exception(res.toString)
  }
}

object SQLParser extends SQLParser