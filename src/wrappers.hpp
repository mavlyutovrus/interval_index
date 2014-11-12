#include <vector>
#include <time.h>
#include <chrono>
#include <iostream>
#include <fstream>
#include <string>
#include <algorithm>
#include <unistd.h>
#include <sstream>
#include <set>
#include <algorithm>
#include <stdio.h>
#include <random>
#include <memory>
#include "time_routines.hpp"
#include <time.h>
#include <set>
#include <unordered_set>

#include <papi.h>

#include "interval_index/interval_index.hpp"
#include "interval_tree/IntervalTree.h"
#include "r_tree/RTree.h"
#include "nclist/intervaldb.h"
#include "segment_tree/segment_tree.hpp"
#include "mem_usage.h"
#define TSharedPtr std::shared_ptr







typedef double TIntervalBorder;
typedef int TValue;
typedef std::pair<TIntervalBorder, TIntervalBorder> TInterval;
typedef std::pair<TInterval, TValue> TKeyId;


class TWrapper {
public:
	virtual void Build(const vector<TKeyId>&, double* timeConsumptionPtr, double* memConsumptionPtr) = 0;
	virtual double CalcQueryTime(const vector<TInterval>&, long long* hitsCountPtr=NULL) = 0;
	virtual void Clear() = 0;
	virtual void TestQuality(const vector<TKeyId>& data, const vector<TInterval>& queries) const {
	}
	virtual ~TWrapper() {
	}
	TWrapper(const string id="") : Id(id)
							  ,	StartRealCycles(0)
	                          , StartRealMicroSec(0)
	                          , StartVirtualCycles(0)
	                          , StartVirtualMicroSec(0)
							  , DeltaRealCycles(0)
							  , DeltaRealMicroSec(0)
							  , DeltaVirtualCycles(0)
							  , DeltaVirtualMicroSec(0)
							  , VirtMemUsageOnStart(0)
							  , ResidenMemUsageOnStart(0)
							  , VirtMemUsageDeltaKb(0)
							  , ResidenMemUsageDeltaKb(0) {}
	string Id;
	long long StartRealCycles, StartRealMicroSec;
	long long StartVirtualCycles, StartVirtualMicroSec;
	long long DeltaRealCycles, DeltaRealMicroSec;
	long long DeltaVirtualCycles, DeltaVirtualMicroSec;
	double VirtMemUsageOnStart, ResidenMemUsageOnStart;
	double VirtMemUsageDeltaKb, ResidenMemUsageDeltaKb;

	void StartTimer() {

		StartRealCycles = PAPI_get_real_cyc();
		StartRealMicroSec = PAPI_get_real_usec();
		StartVirtualCycles = PAPI_get_virt_cyc();
		StartVirtualMicroSec = PAPI_get_virt_usec();
	}
	void StopTimer() {
		DeltaRealCycles = PAPI_get_real_cyc() - StartRealCycles;
		DeltaRealMicroSec = PAPI_get_real_usec() - StartRealMicroSec;
		DeltaVirtualCycles = PAPI_get_virt_cyc() - StartVirtualCycles;
		DeltaVirtualMicroSec = PAPI_get_virt_usec() - StartVirtualMicroSec;
	}

	void StartMeasureMemory() {
		//to be sure, that the values are updated
		process_mem_usage(VirtMemUsageOnStart, ResidenMemUsageOnStart);
		process_mem_usage(VirtMemUsageOnStart, ResidenMemUsageOnStart);
		process_mem_usage(VirtMemUsageOnStart, ResidenMemUsageOnStart);
	}

	void CalcMemoryUsage() {
		process_mem_usage(VirtMemUsageDeltaKb, ResidenMemUsageDeltaKb);
		VirtMemUsageDeltaKb -= VirtMemUsageOnStart;
		ResidenMemUsageDeltaKb -= ResidenMemUsageOnStart;
	}


};


class TResultsCounter {
public:
	TResultsCounter(): Count(0) {

	}
	void operator()(const TInterval& interval, const TValue& value) {
		++Count;
	}
	long long GetCount() const {
		return Count;
	}
private:
	long long Count;

};


template <int SPACE_FACTOR>
class TIntervalIndexWrapper : public TWrapper {
public:
	TIntervalIndexWrapper(const string id) : TWrapper(id)
										   , IntervalIndexPtr(NULL) {

	}
	virtual void Build(const vector<TKeyId>& data, double* timeConsumptionPtr, double* memConsumptionPtr) {
		StartMeasureMemory();
		StartTimer();

		IntervalIndexPtr = TSharedPtr<TIntervalIndex<TIntervalBorder, TValue> >
								(new TIntervalIndex<TIntervalBorder, TValue>(data, SPACE_FACTOR));
		StopTimer();
		CalcMemoryUsage();
		*timeConsumptionPtr = DeltaVirtualMicroSec / 1000.0;
		*memConsumptionPtr = VirtMemUsageDeltaKb;
	}
	virtual void Clear() {
		IntervalIndexPtr = NULL;
	}

	virtual double CalcQueryTime(const vector<TInterval>& queries, long long* hitsCountPtr=NULL) {
		if (!IntervalIndexPtr) {
			return 0.0;
		}
		TTime startTime = GetTime();
		StartTimer();
		TResultsCounter counter;
		for (int queryIndex = 0; queryIndex < queries.size(); ++queryIndex) {
			IntervalIndexPtr->Search(queries[queryIndex].first, queries[queryIndex].second, &counter);
		}
		if (hitsCountPtr) {
			*hitsCountPtr = counter.GetCount();
		}
		StopTimer();
		return GetElapsedInSeconds(startTime, GetTime());
	}
private:
	TSharedPtr<TIntervalIndex<TIntervalBorder, TValue> > IntervalIndexPtr;
};



class TIntervalIndexTester {
public:
	TIntervalIndexTester(const string id,
								   const int spaceFactor=1,
								   const int checkpointInterval=-1) :
									   Id(id),
									   SPACE_FACTOR(spaceFactor),
									   CheckpointInterval(checkpointInterval),
									   IntervalIndexPtr(NULL) {
	}
	void Build(const vector<TKeyId>& data) {
		IntervalIndexPtr = TSharedPtr<TIntervalIndex<TIntervalBorder, TValue> >
							(new TIntervalIndex<TIntervalBorder, TValue>(data, SPACE_FACTOR, CheckpointInterval));
	}
    void Clear() {
		IntervalIndexPtr = NULL;
	}
	double CalcQueryTime(const vector<TInterval>& queries,
								 long long* hitsCountPtr,
								 double* onlyBinarySearchesPtr,
								 double* onlyStartingBeforePtr,
								 double* wholeTimePtr) {
		if (!IntervalIndexPtr) {
			return 0.0;
		}
		*onlyBinarySearchesPtr = 0;
		*onlyStartingBeforePtr = 0;
		*wholeTimePtr = 0;
		*hitsCountPtr = 0;

		{
			TResultsCounter counter;
			TTime startTime = GetTime();
			for (int queryIndex = 0; queryIndex < queries.size(); ++queryIndex) {
				IntervalIndexPtr->Search(queries[queryIndex].first, queries[queryIndex].second, &counter);
			}
			*wholeTimePtr = GetElapsedInSeconds(startTime, GetTime());
			if (hitsCountPtr) {
				*hitsCountPtr += counter.GetCount();
			}
		}
		{
			TResultsCounter counter;
			TTime startTime = GetTime();
			//to prevent over-optimization with 03
			int dummy_counter = 0;
			for (int queryIndex = 0; queryIndex < queries.size(); ++queryIndex) {
				dummy_counter += IntervalIndexPtr->SearchBinarySearchOnly(queries[queryIndex].first, queries[queryIndex].second, &counter);
			}
			*onlyBinarySearchesPtr = GetElapsedInSeconds(startTime, GetTime());
			if (hitsCountPtr) {
				*hitsCountPtr += dummy_counter;
			}
		}

		{
			TResultsCounter counter;
			TTime startTime = GetTime();
			for (int queryIndex = 0; queryIndex < queries.size(); ++queryIndex) {
				IntervalIndexPtr->SearchStartingBeforeOnly(queries[queryIndex].first, queries[queryIndex].second, &counter);
			}
			*onlyStartingBeforePtr = GetElapsedInSeconds(startTime, GetTime());
			if (hitsCountPtr) {
				*hitsCountPtr += counter.GetCount();
			}
		}
		return *wholeTimePtr;
	}

	const string Id;
	const int SPACE_FACTOR;
	const int CheckpointInterval;
	TSharedPtr<TIntervalIndex<TIntervalBorder, TValue> > IntervalIndexPtr;
};





class TIntervalTreeWrapper : public TWrapper {
public:
	TIntervalTreeWrapper(const string id) : TWrapper(id)
										  , ContainerPtr(NULL) {
	}
	virtual void Build(const vector<TKeyId>& data, double* timeConsumptionPtr, double* memConsumptionPtr) {
		vector<Interval<TValue, TIntervalBorder> > points4tree;
		for (int pointIndex = 0; pointIndex < data.size(); ++pointIndex) {
			const TIntervalBorder& start = data[pointIndex].first.first;
			const TIntervalBorder& end = data[pointIndex].first.second;
			const TValue& value = data[pointIndex].second;
			points4tree.push_back(Interval<TValue, TIntervalBorder>(start, end, data[pointIndex].second));
		}

		StartMeasureMemory();
		StartTimer();
		ContainerPtr = TSharedPtr<IntervalTree<TValue, TIntervalBorder> >
		                    (new IntervalTree<TValue, TIntervalBorder>(points4tree));
		StopTimer();
		CalcMemoryUsage();
		*timeConsumptionPtr = DeltaVirtualMicroSec / 1000.0;
		*memConsumptionPtr = VirtMemUsageDeltaKb;
	}
	virtual void Clear() {
		ContainerPtr = NULL;
	}

	virtual void TestQuality(const vector<TKeyId>& data, const vector<TInterval>& queries) const {
		for (int query_index = 0; query_index < 100; ++query_index) {
			int hitsCount = ContainerPtr->findOverlapping(queries[query_index].first, queries[query_index].second);

			set<TKeyId> inside;
			for (auto dataIt = data.begin(); dataIt != data.end(); ++dataIt) {
				if (dataIt->first.second >= queries[query_index].first && dataIt->first.first <= queries[query_index].second) {
					inside.insert(*dataIt);
				}
			}
			if (inside.size() != hitsCount) {
				exit(1);
			}
		}
	}


	virtual double CalcQueryTime(const vector<TInterval>& queries, long long* hitsCountPtr=NULL) {
		TTime startTime = GetTime();
		long long totalHitsCount  = 0;
		StartTimer();
		for (int query_index = 0; query_index < queries.size(); ++query_index) {
			int hitsCount = ContainerPtr->findOverlapping(queries[query_index].first, queries[query_index].second);
			totalHitsCount += hitsCount;
		}
		StopTimer();
		if (hitsCountPtr) {
			*hitsCountPtr = totalHitsCount;
		}
		return GetElapsedInSeconds(startTime, GetTime());
	}

private:
	TSharedPtr<IntervalTree<TValue, TIntervalBorder> > ContainerPtr;
};


class TSegmentTreeCounter {
public:
	TSegmentTreeCounter(): Count(0) {
	}
	void operator()(const int& value) {
		if (value >= ResultsBitMap.size()) {
			ResultsBitMap.resize((value << 1), false);
		}
		if (!ResultsBitMap[value]) {
			ResultsBitMap[value] = true;
			Results.push_back(value);
		}
	}
	long long GetCount() const {
		return Results.size();
	}
	void Refresh() {
		for (int index = 0; index < Results.size(); ++index) {
			ResultsBitMap[Results[index]] = false;
		}
		Results.resize(0);
	}
	std::vector<int> Results;
	std::vector<bool> ResultsBitMap;
private:
	long long Count;

};

class TSegementTreeWrapper : public TWrapper {
public:
	typedef TSegmentTree<TIntervalBorder, TValue> TSegTree;
	typedef typename TSegTree::TKeyValue TSTKeyValue;
	TSegementTreeWrapper(const string id) : TWrapper(id)
										  , ContainerPtr(NULL) {
	}
	virtual void Build(const vector<TKeyId>& data, double* timeConsumptionPtr, double* memConsumptionPtr) {
		vector<TSTKeyValue> points4tree;
		for (int pointIndex = 0; pointIndex < data.size(); ++pointIndex) {
			const TIntervalBorder& start = data[pointIndex].first.first;
			const TIntervalBorder& end = data[pointIndex].first.second + FIX_ADD_TO_INCLUDE_RIGHT_BORDER;
			const TValue& value = data[pointIndex].second;
			points4tree.push_back(TSTKeyValue(start, end, value));
		}

		StartMeasureMemory();
		StartTimer();
		ContainerPtr = TSharedPtr<TSegTree>(new TSegTree(points4tree));
		StopTimer();
		CalcMemoryUsage();
		*timeConsumptionPtr = DeltaVirtualMicroSec / 1000.0;
		*memConsumptionPtr = VirtMemUsageDeltaKb;
	}
	virtual void Clear() {
		ContainerPtr = NULL;
	}

	virtual double CalcQueryTime(const vector<TInterval>& queries, long long* hitsCountPtr=NULL) {
		long long totalHitsCount = 0;
		TSegmentTreeCounter counter;
		TTime startTime = GetTime();
		long long totalHits = 0;
		StartTimer();
		for (int query_index = 0; query_index < queries.size(); ++query_index) {
			ContainerPtr->Search(queries[query_index].first, queries[query_index].second + FIX_ADD_TO_INCLUDE_RIGHT_BORDER, &counter);
			totalHits += counter.Results.size();
			counter.Refresh();
		}
		StopTimer();
		if (hitsCountPtr) {
			*hitsCountPtr = totalHits;
		}
		return GetElapsedInSeconds(startTime, GetTime());
	}

private:
	const double FIX_ADD_TO_INCLUDE_RIGHT_BORDER = 0.000001;
	TSharedPtr<TSegmentTree<TIntervalBorder, TValue> > ContainerPtr;
};


bool RTreeCallback(int id, void* args) {
	return true;
}


class TRTreeWrapper : public TWrapper {
public:
	typedef RTree<int, double, 1, double> TRTree;
	TRTreeWrapper(const string id) : TWrapper(id), ContainerPtr(NULL) {
	}
	virtual void Build(const vector<TKeyId>& data, double* timeConsumptionPtr, double* memConsumptionPtr) {
		StartMeasureMemory();
		StartTimer();
		ContainerPtr = TSharedPtr<TRTree>(new TRTree);
		for (int pointIndex = 0; pointIndex < data.size(); ++pointIndex) {
			double start[1] = { data[pointIndex].first.first };
			double end[1] = { data[pointIndex].first.second };
			ContainerPtr->Insert(start, end, data[pointIndex].second);
		}
		StopTimer();
		CalcMemoryUsage();
		*timeConsumptionPtr = DeltaVirtualMicroSec / 1000.0;
		*memConsumptionPtr = VirtMemUsageDeltaKb;
	}
	virtual void Clear() {
		ContainerPtr = NULL;
	}

	virtual double CalcQueryTime(const vector<TInterval>& queries, long long* hitsCountPtr=NULL) {
		TTime startTime = GetTime();
		long long totalHitsCount = 0;
		StartTimer();
		for (int queryIndex = 0; queryIndex < queries.size(); ++queryIndex) {
			double start[1] = { queries[queryIndex].first};
			double end[1] = { queries[queryIndex].second};
			int hitsCount = ContainerPtr->Search(start, end, RTreeCallback, NULL);
			totalHitsCount += hitsCount;
		}
		StopTimer();
		if (hitsCountPtr) {
			*hitsCountPtr = totalHitsCount;
		}
		return GetElapsedInSeconds(startTime, GetTime());
	}

private:
	TSharedPtr<TRTree> ContainerPtr;

};


const int DOUBLE2INT_MULT = 100;

class TNClistWrapper : public TWrapper {
public:
	TNClistWrapper(const string id) : TWrapper(id),
									  ContainerPtr(NULL),
									  p_n(0),
									  p_nlists(0) {
	}
	virtual void Build(const vector<TKeyId>& data, double* timeConsumptionPtr, double* memConsumptionPtr) {
		Clear();
		for (int pointIndex = 0; pointIndex < data.size(); ++pointIndex) {
			IntervalMap interval;
			interval.start = data[pointIndex].first.first * DOUBLE2INT_MULT;
			interval.end = data[pointIndex].first.second * DOUBLE2INT_MULT;
			interval.sublist = -1;
			interval.target_id = pointIndex + 1;
			interval.target_start = 0;
			interval.target_end = 0;
			Points4Nclist.push_back(interval);
		}
		StartMeasureMemory();
		StartTimer();
		ContainerPtr = TSharedPtr<SublistHeader>(build_nested_list(&Points4Nclist[0],
															 Points4Nclist.size(),
															 &p_n,
															 &p_nlists));
		StopTimer();
		CalcMemoryUsage();
		*timeConsumptionPtr = DeltaVirtualMicroSec / 1000.0;
		*memConsumptionPtr = VirtMemUsageDeltaKb;
	}
	virtual void Clear() {
		ContainerPtr = NULL;
		Points4Nclist.clear();
	}

	virtual double CalcQueryTime(const vector<TInterval>& queries, long long* hitsCountPtr=NULL) {
		const int BUFFER_SIZE = 10000;
		IntervalMap buffer[BUFFER_SIZE];

		TTime startTime = GetTime();
		long long totalHitsCount = 0;
		StartTimer();
		for (int queryIndex = 0; queryIndex < queries.size(); ++queryIndex) {
			int start = queries[queryIndex].first * DOUBLE2INT_MULT;
			int end = queries[queryIndex].second * DOUBLE2INT_MULT;
			IntervalIterator* iterator = 0;
			int p_nreturn;
			find_intervals(iterator,
							start,
							end,
							&(Points4Nclist.at(0)),
							p_n,
							ContainerPtr.get(),
							p_nlists,
							&buffer[0],
							BUFFER_SIZE,
							&p_nreturn,
							&iterator
							);
			totalHitsCount += p_nreturn;
		}
		StopTimer();
		if (hitsCountPtr) {
			*hitsCountPtr = totalHitsCount;
		}
		return GetElapsedInSeconds(startTime, GetTime());
	}

private:
	TSharedPtr<SublistHeader> ContainerPtr;
	vector<IntervalMap> Points4Nclist;
	int p_n, p_nlists;
};

#include "rstar_tree/RStarTree.h"
#include "rstar_tree/RStarVisitor.h"



class TRStarTreeWrapper : public TWrapper {
public:
	typedef RStarTree<int, 1, 32, 64> TRStarTree;
	struct Visitor {
		long long Count;
		bool ContinueVisiting;
		Visitor() : Count(0), ContinueVisiting(true) {};
		void operator()(const TRStarTree::Leaf * const leaf) {
			++Count;
		}
	};
	TRStarTreeWrapper(const string id) : TWrapper(id), ContainerPtr(NULL) {
	}
	virtual void Build(const vector<TKeyId>& data, double* timeConsumptionPtr, double* memConsumptionPtr) {
		StartMeasureMemory();
		StartTimer();
		ContainerPtr = TSharedPtr<TRStarTree>(new TRStarTree);
		for (int pointIndex = 0; pointIndex < data.size(); ++pointIndex) {
			TRStarTree::BoundingBox interval;
			interval.edges[0].first = data[pointIndex].first.first * DOUBLE2INT_MULT;
			interval.edges[0].second = data[pointIndex].first.second * DOUBLE2INT_MULT;
			ContainerPtr->Insert(pointIndex + 1, interval);
		}
		StopTimer();
		CalcMemoryUsage();
		*timeConsumptionPtr = DeltaVirtualMicroSec / 1000.0;
		*memConsumptionPtr = VirtMemUsageDeltaKb;
	}
	virtual void Clear() {
		ContainerPtr = NULL;
	}

	virtual double CalcQueryTime(const vector<TInterval>& queries, long long* hitsCountPtr=NULL) {
		TRStarTree::BoundingBox queryInterval;
		Visitor results;

		TTime startTime = GetTime();
		StartTimer();
		for (int queryIndex = 0; queryIndex < queries.size(); ++queryIndex) {
			queryInterval.edges[0].first = queries[queryIndex].first * DOUBLE2INT_MULT;
			queryInterval.edges[0].second = queries[queryIndex].second * DOUBLE2INT_MULT;
			ContainerPtr->Query(TRStarTree::AcceptOverlapping(queryInterval), results);
		}
		StopTimer();
		if (hitsCountPtr) {
			*hitsCountPtr = results.Count;
		}
		return GetElapsedInSeconds(startTime, GetTime());
	}

private:
	TSharedPtr<TRStarTree> ContainerPtr;

};
