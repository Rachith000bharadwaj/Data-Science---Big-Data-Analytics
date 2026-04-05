import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.lib._
import java.io.{File, PrintWriter}

object GraphXAnalytics {
  def main(args: Array[String]): Unit = {
    // ================================================================
    //  1. SPARK SESSION SETUP
    // ================================================================
    val spark = SparkSession.builder
      .appName("GraphX Deep Analytics - BDA Project")
      .config("spark.driver.memory", "4g")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()
      
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    
    val tStart = System.currentTimeMillis()
    val timings = scala.collection.mutable.LinkedHashMap[String, Double]()
    
    // 2. Dynamic Dataset Discovery
    val outDir = "GraphX/outputs"
    new File(outDir).mkdirs()
    
    // Read dataset path from config file (written by run_comparison.py)
    // Falls back to auto-detect if config file doesn't exist
    val configFile = new File("GraphX/.dataset_config")
    val dataPath = if (configFile.exists()) {
      val path = scala.io.Source.fromFile(configFile).getLines().next().trim
      println(s"  [Config] Reading dataset from .dataset_config: $path")
      path
    } else if (args.length > 0 && new File(args(0)).exists()) {
      args(0)
    } else {
      val dataDir = new File("data_lake")
      val candidates = dataDir.listFiles()
        .filter(f => f.getName.endsWith(".txt") || f.getName.endsWith(".txt.gz") ||
                     f.getName.endsWith(".csv") || f.getName.endsWith(".csv.gz") ||
                     f.getName.endsWith(".tsv") || f.getName.endsWith(".tsv.gz"))
        .sortBy(_.length())
      if (candidates.isEmpty) {
        println("[ERROR] No dataset found in data_lake/")
        System.exit(1)
      }
      candidates.head.getPath
    }
    val datasetName = new File(dataPath).getName
      .replaceAll("\\.(txt|csv|tsv)(\\.gz)?$", "")
      .replace("_", " ").replace("-", " ")
    
    println("=" * 60)
    println("  BDA Social Graph -- Native GraphX (Scala) Deep Analytics")
    println(s"  Dataset: $datasetName ($dataPath)")
    println("=" * 60)
    
    // ================================================================
    //  3. GRAPH LOADING & CONSTRUCTION
    // ================================================================
    println("\n[PHASE 1] LOADING GRAPH FROM SNAP DATASET...")
    val loadStart = System.currentTimeMillis()
    val rawLines = sc.textFile(dataPath)
    
    val edges: RDD[Edge[Int]] = rawLines
      .filter(line => !line.startsWith("#") && line.trim.nonEmpty)
      .flatMap { line =>
        val parts = line.split("\\s+")
        val src = parts(0).toLong
        val dst = parts(1).toLong
        if (src != dst) {
          Array(Edge(src, dst, 1), Edge(dst, src, 1))
        } else {
          Array.empty[Edge[Int]]
        }
      }.distinct()
      
    val graph = Graph.fromEdges(edges, 1).cache()
    
    val nVertices = graph.vertices.count()
    val nEdges = graph.edges.count() / 2
    val loadTime = (System.currentTimeMillis() - loadStart) / 1000.0
    timings("Graph Loading") = loadTime
    
    println(s"  [OK] Graph loaded in $loadTime s")
    println(f"    Nodes: $nVertices%,d")
    println(f"    Edges: $nEdges%,d (undirected)")
    
    // ================================================================
    //  4. MODULE 1: PAGERANK (Influence Maximization)
    // ================================================================
    println("\n" + "=" * 60)
    println("  MODULE 1: PAGERANK -- Influence Maximization")
    println("=" * 60)
    val prStart = System.currentTimeMillis()
    val prGraph = graph.pageRank(0.0001)
    val topPR = prGraph.vertices.sortBy(_._2, ascending = false).take(10)
    val prTime = (System.currentTimeMillis() - prStart) / 1000.0
    timings("PageRank") = prTime
    
    println(s"  [OK] PageRank completed in $prTime s")
    println("  Top 10 Influential Nodes:")
    topPR.zipWithIndex.foreach { case ((id, score), i) =>
      println(f"    ${i+1}%2d. Node $id%5d  Score=$score%.6f")
    }
    
    // Save PageRank CSV
    val prWriter = new PrintWriter(new File(s"$outDir/pagerank_top100.csv"))
    prWriter.println("node_id,pagerank_score")
    prGraph.vertices.sortBy(_._2, ascending = false).take(100).foreach { case (id, score) =>
      prWriter.println(s"$id,$score")
    }
    prWriter.close()
    
    // ================================================================
    //  5. MODULE 2: LABEL PROPAGATION (Community Detection)
    // ================================================================
    println("\n" + "=" * 60)
    println("  MODULE 2: LABEL PROPAGATION -- Community Detection")
    println("=" * 60)
    val lpaStart = System.currentTimeMillis()
    val lpaGraph = LabelPropagation.run(graph, 5)
    val communityRDD = lpaGraph.vertices.map(_._2)
    val nCommunities = communityRDD.distinct().count()
    
    // Community size distribution
    val commSizes = lpaGraph.vertices.map { case (_, label) => (label, 1) }
      .reduceByKey(_ + _)
      .map(_._2)
      .collect()
      .sorted
      .reverse
    
    val lpaTime = (System.currentTimeMillis() - lpaStart) / 1000.0
    timings("Label Propagation") = lpaTime
    
    println(s"  [OK] LPA completed in $lpaTime s")
    println(s"  Total Communities: $nCommunities")
    println(s"  Largest 5 communities: ${commSizes.take(5).mkString(", ")}")
    println(s"  Communities with >10 nodes: ${commSizes.count(_ > 10)}")
    println(s"  Singleton communities: ${commSizes.count(_ == 1)}")
    
    // Save community assignment CSV
    val lpaWriter = new PrintWriter(new File(s"$outDir/community_assignment.csv"))
    lpaWriter.println("node_id,community_id")
    lpaGraph.vertices.collect().foreach { case (id, label) =>
      lpaWriter.println(s"$id,$label")
    }
    lpaWriter.close()
    
    // Save community sizes CSV
    val commSizeWriter = new PrintWriter(new File(s"$outDir/community_sizes.csv"))
    commSizeWriter.println("community_id,size")
    lpaGraph.vertices.map { case (_, label) => (label, 1) }
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .collect()
      .foreach { case (label, size) =>
        commSizeWriter.println(s"$label,$size")
      }
    commSizeWriter.close()
    
    // ================================================================
    //  6. MODULE 3: TRIANGLE COUNT (Clustering)
    // ================================================================
    println("\n" + "=" * 60)
    println("  MODULE 3: TRIANGLE COUNT -- Clustering Coefficient")
    println("=" * 60)
    val tcStart = System.currentTimeMillis()
    val partedGraph = graph.partitionBy(PartitionStrategy.RandomVertexCut)
    val tcGraph = partedGraph.triangleCount()
    val triangleCounts = tcGraph.vertices.map(_._2)
    val totalTriangles = triangleCounts.sum() / 3
    
    // Global Clustering Coefficient = 3 * triangles / connected_triples
    // Connected triples = sum over v of (degree(v) choose 2)
    val degreesRDD = graph.degrees
    val connectedTriples = degreesRDD.map { case (_, deg) =>
      deg.toLong * (deg.toLong - 1) / 2
    }.sum()
    
    val globalCC = if (connectedTriples > 0) (3.0 * totalTriangles) / connectedTriples else 0.0
    
    // Average local clustering coefficient
    val localCC = tcGraph.vertices.innerJoin(degreesRDD) { (vid, tri, deg) =>
      val possibleTriangles = deg.toLong * (deg.toLong - 1) / 2
      if (possibleTriangles > 0) tri.toDouble / possibleTriangles else 0.0
    }
    val avgLocalCC = localCC.map(_._2).sum() / nVertices
    
    val tcTime = (System.currentTimeMillis() - tcStart) / 1000.0
    timings("Triangle Count") = tcTime
    
    println(s"  [OK] Triangle Count completed in $tcTime s")
    println(f"  Total Triangles: $totalTriangles%,.0f")
    println(f"  Connected Triples: $connectedTriples%,.0f")
    println(f"  Global Clustering Coefficient: $globalCC%.6f")
    println(f"  Average Local Clustering Coefficient: $avgLocalCC%.6f")
    
    // ================================================================
    //  7. MODULE 4: CONNECTED COMPONENTS (Connectivity)
    // ================================================================
    println("\n" + "=" * 60)
    println("  MODULE 4: CONNECTED COMPONENTS -- Connectivity")
    println("=" * 60)
    val ccStart = System.currentTimeMillis()
    val ccGraph = graph.connectedComponents()
    val nComponents = ccGraph.vertices.map(_._2).distinct().count()
    val componentSizes = ccGraph.vertices.map { case (_, comp) => (comp, 1) }
      .reduceByKey(_ + _)
      .map(_._2)
      .collect()
      .sorted
      .reverse
    val ccTime = (System.currentTimeMillis() - ccStart) / 1000.0
    timings("Connected Components") = ccTime
    
    println(s"  [OK] Connected Components completed in $ccTime s")
    println(s"  Total Components: $nComponents")
    println(s"  Largest component: ${componentSizes.head} nodes")
    if (componentSizes.length > 1) {
      println(s"  Component sizes: ${componentSizes.take(5).mkString(", ")}")
    }
    
    // ================================================================
    //  8. MODULE 5: DEGREE ANALYSIS (Network Structure)
    // ================================================================
    println("\n" + "=" * 60)
    println("  MODULE 5: DEGREE ANALYSIS -- Network Structure")
    println("=" * 60)
    val degStart = System.currentTimeMillis()
    
    val degreeValues = degreesRDD.map(_._2.toDouble).cache()
    val degreeCount = degreeValues.count()
    val maxDegree = degreeValues.max().toInt
    val minDegree = degreeValues.min().toInt
    val avgDegree = degreeValues.sum() / degreeCount
    val variance = degreeValues.map(d => (d - avgDegree) * (d - avgDegree)).sum() / degreeCount
    val stdDegree = math.sqrt(variance)
    
    // Degree distribution histogram
    val degreeHist = degreesRDD.map { case (_, deg) => (deg, 1) }
      .reduceByKey(_ + _)
      .sortByKey()
      .collect()
    
    // Top 10 hub nodes
    val topHubs = degreesRDD.sortBy(_._2, ascending = false).take(10)
    
    // Graph density = 2E / (V * (V-1))
    val density = (2.0 * nEdges) / (nVertices * (nVertices - 1))
    
    val degTime = (System.currentTimeMillis() - degStart) / 1000.0
    timings("Degree Analysis") = degTime
    
    println(s"  [OK] Degree Analysis completed in $degTime s")
    println(f"  Graph Density: $density%.6f")
    println(f"  Max Degree: $maxDegree")
    println(f"  Min Degree: $minDegree")
    println(f"  Avg Degree: $avgDegree%.2f")
    println(f"  Std Deviation: $stdDegree%.2f")
    println("  Top 10 Hub Nodes:")
    topHubs.zipWithIndex.foreach { case ((id, deg), i) =>
      println(f"    ${i+1}%2d. Node $id%5d  Degree=$deg")
    }
    
    // Save degree distribution CSV
    val degWriter = new PrintWriter(new File(s"$outDir/degree_distribution.csv"))
    degWriter.println("degree,count")
    degreeHist.foreach { case (deg, count) =>
      degWriter.println(s"$deg,$count")
    }
    degWriter.close()
    
    // Save top hubs CSV
    val hubWriter = new PrintWriter(new File(s"$outDir/top_hubs.csv"))
    hubWriter.println("node_id,degree")
    degreesRDD.sortBy(_._2, ascending = false).take(100).foreach { case (id, deg) =>
      hubWriter.println(s"$id,$deg")
    }
    hubWriter.close()
    
    // ================================================================
    //  9. MODULE 6: SHORTEST PATHS (Reachability)
    // ================================================================
    println("\n" + "=" * 60)
    println("  MODULE 6: SHORTEST PATHS -- Reachability")
    println("=" * 60)
    val spStart = System.currentTimeMillis()
    val landmarks = Seq(topPR(0)._1, topPR(1)._1)
    println(s"  Landmarks: ${landmarks.mkString(", ")}")
    val spGraph = ShortestPaths.run(graph, landmarks)
    
    // Compute diameter estimate (max shortest path among sampled nodes)
    val pathLengths = spGraph.vertices.flatMap { case (_, pathMap) =>
      pathMap.values.map(_.toDouble)
    }
    val maxPathLen = if (!pathLengths.isEmpty()) pathLengths.max().toInt else 0
    val avgPathLen = if (!pathLengths.isEmpty()) pathLengths.sum() / pathLengths.count() else 0.0
    
    val spTime = (System.currentTimeMillis() - spStart) / 1000.0
    timings("Shortest Paths") = spTime
    
    println(s"  [OK] Shortest Paths completed in $spTime s")
    println(s"  Estimated Diameter (from landmarks): $maxPathLen")
    println(f"  Average Path Length (from landmarks): $avgPathLen%.2f")
    
    // ================================================================
    //  10. SUMMARY & BENCHMARK EXPORT
    // ================================================================
    val totalTime = (System.currentTimeMillis() - tStart) / 1000.0
    timings("TOTAL") = totalTime
    
    println("\n" + "=" * 60)
    println("               GRAPHX DEEP ANALYTICS - SUMMARY")
    println("=" * 60)
    println(f"  Nodes:                    $nVertices%,d")
    println(f"  Edges:                    $nEdges%,d")
    println(f"  Density:                  $density%.6f")
    println(f"  Triangles:                $totalTriangles%,.0f")
    println(f"  Global Clustering Coeff:  $globalCC%.6f")
    println(f"  Avg Local Clustering:     $avgLocalCC%.6f")
    println(f"  Communities (LPA):        $nCommunities")
    println(f"  Connected Components:     $nComponents")
    println(f"  Max Degree:               $maxDegree")
    println(f"  Avg Degree:               $avgDegree%.2f")
    println(f"  Estimated Diameter:       $maxPathLen")
    println(f"  Avg Path Length:          $avgPathLen%.2f")
    
    println("\n  Per-Algorithm Timings:")
    println("  " + "-" * 45)
    timings.foreach { case (name, time) =>
      println(f"    $name%-25s $time%8.2f s")
    }
    println("  " + "-" * 45)
    println(f"    TOTAL EXECUTION TIME:    $totalTime%8.2f s")
    
    // Save benchmark CSV
    val benchWriter = new PrintWriter(new File(s"$outDir/benchmark_timings.csv"))
    benchWriter.println("algorithm,time_seconds")
    timings.foreach { case (name, time) =>
      benchWriter.println(s"$name,$time")
    }
    benchWriter.close()
    
    // Save full summary CSV
    val sumWriter = new PrintWriter(new File(s"$outDir/graph_summary.csv"))
    sumWriter.println("metric,value")
    sumWriter.println(s"nodes,$nVertices")
    sumWriter.println(s"edges,$nEdges")
    sumWriter.println(f"density,$density%.6f")
    sumWriter.println(f"triangles,$totalTriangles%.0f")
    sumWriter.println(f"global_clustering_coeff,$globalCC%.6f")
    sumWriter.println(f"avg_local_clustering_coeff,$avgLocalCC%.6f")
    sumWriter.println(s"communities_lpa,$nCommunities")
    sumWriter.println(s"connected_components,$nComponents")
    sumWriter.println(s"max_degree,$maxDegree")
    sumWriter.println(f"avg_degree,$avgDegree%.2f")
    sumWriter.println(s"estimated_diameter,$maxPathLen")
    sumWriter.println(f"avg_path_length,$avgPathLen%.2f")
    sumWriter.println(f"total_time_seconds,$totalTime%.2f")
    sumWriter.close()
    
    println(s"\n  All outputs saved to: $outDir/")
    println("  Files: pagerank_top100.csv, community_assignment.csv,")
    println("         community_sizes.csv, degree_distribution.csv,")
    println("         top_hubs.csv, benchmark_timings.csv, graph_summary.csv")
    println("\n  GraphX Deep Analytics Complete.")
    
    spark.stop()
  }
}

// Spark-shell wrapper execution
GraphXAnalytics.main(Array.empty)
