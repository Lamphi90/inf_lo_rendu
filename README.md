# inf_lo_rendu
SON algorithm implementation to get Frequent itemsets
We make use of Spark Parallelism - using mapPartition function to get results in around 45 seconds. 
•	Data Creation: I have joined users on ratings data sets on userId and grouped them by gender. I took the joinedMale data for case1 and joinedFemale data for case2.
•	Basket Creation : Once I have the data , I create baskets by mapping the data according to the caseNo
•	Pass the basket created in above step to mapPartitions to run my user defined apriori fun by passing partition and the calculated partition threshold
•	Inside my apriori, I first calculated freq singletons.  I add these frequent singletons to my solution.  we start with L_set as  itemsets of size K (in our case it would be already calculated singletons before starting the iterations)  we create itemsets of size K+1 . We loop baskets to count Itemsets of size K+1 and then the candidates which are greater than partition threshold get added to the solution. Now my L_set for k+1 iteration will be updated with Candidates of K step and the iterations are continued until I reach an empty set for L_set.  This is whole first step of SON.
•	I reduce the output from above step where I concatenate lists of freq  items for size K coming from each of the map partitions and reduce this list to a set to avoid duplicates
•	In my second map of SON, I pass my basket to map partitions to calculate the count of frequent items.  In my second reduce of SON, I aggregate the counts for each of the frequent itemset and filter the elements which have count greater or equal to the support threshold of the input. 
