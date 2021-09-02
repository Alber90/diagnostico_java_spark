package minsait.ttaa.datio.engine;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import minsait.ttaa.datio.common.Common;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static minsait.ttaa.datio.engine.Constants.*;
import static org.apache.spark.sql.functions.*;

public class Transformer extends Writer {
	private SparkSession spark;
	ReadProperties readP = new ReadProperties();
	public Transformer(@NotNull SparkSession spark) {
		this.spark = spark;
		Dataset<Row> df = readInput();

		df = cleanData(df);
		df = exampleWindowFunction(df);
		df = columnSelection(df);

		// for show 100 records after your transformations and show the Dataset schema
		df.show(100, false);
		df.printSchema();

		// Uncomment when you want write your final output
		// write(df);
		df.coalesce(1);
		df.write().format("parquet").save(readP.getOUTPUT_PARQUET());
	}

	private Dataset<Row> columnSelection(Dataset<Row> df) {
		return df.select(
				// shortName.column(),
				// longName.column(),
				// age.column(),
				// heightCm.column(),
				// weight_kg.column(),
				nationality.column(),
				// clubName.column(),
				overall.column(), potential.column(), teamPosition.column(),
				// catHeightByPosition.column(),
				ageRange.column(), rankByNationalityPosition.column(), potentialVsOverall.column());
	}

	/**
	 * @return a Dataset readed from csv file
	 */
	private Dataset<Row> readInput() {
		Dataset<Row> df = spark.read().option(HEADER, true).option(INFER_SCHEMA, true).csv(readP.getINPUT_PATH_CSV());
		return df;
	}

	/**
	 * @param df
	 * @return a Dataset with filter transformation applied column team_position !=
	 *         null && column short_name != null && column overall != null
	 */
	private Dataset<Row> cleanData(Dataset<Row> df) {
		df = df.filter(teamPosition.column().isNotNull().and(shortName.column().isNotNull())
				.and(overall.column().isNotNull()));

		return df;
	}

	/**
	 * @param df is a Dataset with players information (must have team_position and
	 *           height_cm columns)
	 * @return add to the Dataset the column "cat_height_by_position" by each
	 *         position value cat A for if is in 20 players tallest cat B for if is
	 *         in 50 players tallest cat C for the rest
	 */
	private Dataset<Row> exampleWindowFunction(Dataset<Row> df) {
		WindowSpec w = Window.partitionBy(nationality.column(), teamPosition.column()).orderBy(overall.column().desc());
		Column rank = rank().over(w);
		Column rule = when(rank.$less(10), "A").when(rank.$less(50), "B").otherwise("C");
		df = addAgeRAnge(df);
		df = divOption(df);
		df = df.withColumn(rankByNationalityPosition.getName(), row_number().over(w));
		df = df.withColumn(catHeightByPosition.getName(), rule);
		df = filterOption(df);
		return df;
	}
	
	private Dataset<Row> filterOption(Dataset<Row> df) {
		df = df.filter(ageRange.column().isin(AGE_OPTION_C, AGE_OPTION_A)
				.and(potentialVsOverall.column().$greater(1.15F))
				.or(ageRange.column().equalTo(AGE_OPTION_B).and(potentialVsOverall.column().$greater(1.5F)))				
				.or(ageRange.column().equalTo(AGE_OPTION_D).and(rankByNationalityPosition.column().$less(5))));
		return df;
	}
	private Dataset<Row> divOption(Dataset<Row> df) {
		Column ruleDiv = potential.column().$div(overall.column());
		df = df.withColumn(potentialVsOverall.getName(), ruleDiv);
		return df;
	}

	private Dataset<Row> addAgeRAnge(Dataset<Row> df) {
		Column ruleAge = when(col(age.getName()).$less(23), AGE_OPTION_A)
				.when(col(age.getName()).$less(27), AGE_OPTION_B).when(col(age.getName()).$less(32), AGE_OPTION_C)
				.otherwise(AGE_OPTION_D);
		df = df.withColumn(ageRange.getName(), ruleAge);
		return df;
	}
}
