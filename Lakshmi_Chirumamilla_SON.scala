
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer
import scala.math.Ordering.Implicits._
import org.apache.spark.rdd.RDD
import java.io._

object Lakshmi_Chirumamilla_SON {   
  
  var Singletons = collection.mutable.Set[Int]()
  var sum = ListBuffer[Int]() 
  var i : Float =0
  
    def main(args: Array[String]){
     val start_time = System.nanoTime()
     
     val conf = new SparkConf().setAppName("HomeWork2").setMaster("local[*]")
     val sparkContext = new SparkContext(conf)
     if (args.length <4) 
        {
          println("Please provide sufficient arguments from command line as per Description file")
          System.exit(1)
        } 
     
     val caseNo = args(0).toInt
      val  usersDat = sparkContext.textFile(args(2))
      val  ratingsDat = sparkContext.textFile(args(1))
      val supportInput = args(3).toInt 
     
     
      val split_Users = usersDat.map(line=>line.split("\\::")).map(line=>(line(0).toInt, line(1).toString))        
      val groupedRDD = split_Users.groupBy(_._2).collect()
      var maleIndex = 0 
      var femaleIndex = 1  
    
         if(groupedRDD(0)._1 == "F")
        {           
          maleIndex = 1
          femaleIndex = 0          
        }
     val maleRDD =sparkContext.parallelize(groupedRDD(maleIndex)._2.toList)
     val femaleRDD = sparkContext.parallelize(groupedRDD(femaleIndex)._2.toList)            
     val split_Ratings = ratingsDat.map(line=>line.split("\\::")).map(line=>(line(0).toInt, line(1).toInt))   
      val rating_items = split_Ratings.map(x=>(x._1,x))
    
     val MaleRatings = maleRDD.join(rating_items).map(t=>(t._1, t._2._2._2))
     val FemaleRatings = femaleRDD.join(rating_items).map(t=>(t._2._2._2,t._1 ))
       var basket : RDD[(Int,Iterable[Int])]= sparkContext.emptyRDD
       var count :Long = 0
       var numPartitions :Int =0 
       var percent : Float = 0       
       if(caseNo ==1)
       {         
         numPartitions = MaleRatings.getNumPartitions
         count = MaleRatings.count() 
         MaleRatings.foreachPartition(x => sum.+=(x.length))   
         basket = MaleRatings.distinct().groupByKey().cache()
       }
       else
       {         
         numPartitions = FemaleRatings.getNumPartitions
         count = FemaleRatings.count() 
         FemaleRatings.foreachPartition(x => sum.+=(x.length)) 
         basket = FemaleRatings.distinct().groupByKey().cache()   
       }
     
     percent = (sum.foldLeft(0)(_ + _))/numPartitions
     val mapPartition = basket.mapPartitions(partition => aPriori(partition,(percent*supportInput)/count),true)    
     val firstReduce =  mapPartition.reduceByKey((x,y) => x:::y,1)  
      val secondMap = firstReduce.mapValues(x => x.toSet).collect()  
      val candidate = sparkContext.broadcast(secondMap)


    val final_counts = basket.mapPartitions(partition => countTotalBasket(partition,candidate.value),true)   
    val secondReduce = final_counts.reduceByKey((x,y) => x+y,1).filter(x => x._2>= supportInput) 
     var final_sorted_grouped = secondReduce.map(x => (x._1.size,x._1.toList.sorted)).sortByKey().groupByKey().toLocalIterator
      
    var freq_list = ListBuffer[List[Int]]()
     var tempHashMap = collection.mutable.HashMap[Int,ListBuffer[List[Int]]]()
    for(elem <- final_sorted_grouped)
    {
      for(j <- elem._2)
      {
        freq_list +=(j)
      }
     
      tempHashMap.put(elem._1,freq_list.sorted)
      freq_list.clear()
    }

     val output = new PrintWriter(new File("Lakshmi_Chirumamilla_SON.case"+caseNo.toString+"_"+supportInput.toString+".txt"))
     
    for((k,v) <- collection.immutable.ListMap(tempHashMap.toSeq.sortWith(_._1 < _._1):_*))
    {
      var counter = v.size
      var j =1
      for(i <- v) {

         output.write(i.mkString("(", ",", ")"))
        j = j+ 1

        if(j<=counter)
          output.write(", ")
      }
      output.write( System.getProperty("line.separator") )
      output.write( System.getProperty("line.separator") )
    }
      output.close()
    candidate.unpersist
    candidate.destroy
    val End_time = System.nanoTime()
    println("Total time :"+(End_time-start_time)/1e9d);
    }
    
    
      def aPriori(basket : Iterator[(Int,Iterable[Int])],threshold : Float): Iterator[(Int, List[collection.mutable.Set[Int]])] = 
    {
      var L_set = collection.mutable.Set[collection.mutable.Set[Int]]()
      var C_set = collection.mutable.Set[collection.mutable.Set[Int]]()
      var solution = scala.collection.mutable.Map[Int, scala.collection.mutable.Set[collection.mutable.Set[Int]]]()
      var items = scala.collection.mutable.Map[Int, Int]().withDefaultValue(0)
      var basketSets = scala.collection.mutable.ListBuffer[collection.mutable.Set[Int]]()
      
     
       basket.foreach { x =>
      x._2.foreach{y =>
        items(y) += 1
      }

      basketSets.+=(x._2.to[collection.mutable.Set])
    }
      
    for ((k, v) <- items) {
      if (v >= threshold) {
        L_set.+=(collection.mutable.Set(k))     
        Singletons += k
      }
    }
        
      var k = 2
    solution.put(k - 1, L_set)     
    while (!L_set.isEmpty) {
      L_set = createItemSets(L_set, k)  //creates I of size K and L_set is updated   
      C_set = getFreqItemSets(L_set, basketSets, threshold)     
      if (!C_set.isEmpty)
        solution.put(k, C_set)
      L_set = C_set
      k = k + 1
    }
    
    var retValue = new ListBuffer[(Int, List[collection.mutable.Set[Int]])]()
    for((k,v)<- solution){     
      retValue.+=((k,v.toList))
    }

      
       return retValue.toIterator 

}
   def createItemSets(L_set : collection.mutable.Set[collection.mutable.Set[Int]] , k : Int ) : collection.mutable.Set[collection.mutable.Set[Int]] ={ 

    var res = L_set.toVector.combinations(2).map(x => x(0) union x(1)).filter(_.size == k).to[collection.mutable.Set]
    return res
  }
       
       def getFreqItemSets(L_set:collection.mutable.Set[collection.mutable.Set[Int]],basketSets : scala.collection.mutable.ListBuffer[collection.mutable.Set[Int]], threshold : Float):collection.mutable.Set[collection.mutable.Set[Int]] ={
    var  freqItemSets = collection.mutable.Set[collection.mutable.Set[Int]]()
    var countL_set = collection.mutable.HashMap[collection.mutable.Set[Int], Int]().withDefaultValue(0)

    for (b <- basketSets) {
      for (elem <- L_set){
        if (elem.subsetOf(b))
         countL_set(elem) += 1
      }
    }

    for ((k,v) <- countL_set)
    {
      if(v >= threshold)
        freqItemSets.+=(k)
    }
    return freqItemSets
  }
   
   def countTotalBasket(basket : Iterator[(Int,Iterable[Int])],candidate : Array[(Int,Set[collection.mutable.Set[Int]])]): Iterator[(collection.mutable.Set[Int],Int)] = {
    var count = collection.mutable.HashMap[collection.mutable.Set[Int], Int]().withDefaultValue(0) 
    for(b <- basket) {
      if (!Singletons.intersect(b._2.to[collection.mutable.Set]).isEmpty) {

        for (c <- candidate) {
          for (i <- c._2) {
            if (i.subsetOf(b._2.toSet)) {
             count(i) += 1

            }
          }
        }
      }
    }

    return count.toIterator

  }

}
    
   
      


  