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

#include "interval_index/interval_index.hpp"
#include "interval_tree/IntervalTree.h"
#include "r_tree/RTree.h"

#define TSharedPtr std::shared_ptr




typedef double TIntervalBorder;
typedef int TValue;
typedef std::pair<TIntervalBorder, TIntervalBorder> TInterval;
typedef std::pair<TInterval, TValue> TKeyId;


struct TWrapper {
	virtual void Build(const vector<TKeyId>&) = 0;
	virtual double CalcQueryTime(const vector<TInterval>&, long long* hitsCountPtr=NULL) const = 0;
	virtual void Clear() = 0;
	virtual void TestQuality(const vector<TKeyId>& data, const vector<TInterval>& queries) const {
	}
	virtual ~TWrapper() {
	}
	TWrapper(const string id) : Id(id) {
	}
	TWrapper() : Id("") {
	}
	string Id;
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




class TIntervalIndexWrapper : public TWrapper {
public:
	TIntervalIndexWrapper(const string id) : TWrapper(id)
										   , IntervalIndexPtr(NULL) {

	}
	virtual void Build(const vector<TKeyId>& data) {
		IntervalIndexPtr = TSharedPtr<TIntervalIndex<TIntervalBorder, TValue> >
								(new TIntervalIndex<TIntervalBorder, TValue>(data));
	}
	virtual void Clear() {
		IntervalIndexPtr = NULL;
	}

	virtual double CalcQueryTime(const vector<TInterval>& queries, long long* hitsCountPtr=NULL) const {
		if (!IntervalIndexPtr) {
			return 0.0;
		}
		TTime startTime = GetTime();
		TResultsCounter counter;
		for (int queryIndex = 0; queryIndex < queries.size(); ++queryIndex) {
			IntervalIndexPtr->Search(queries[queryIndex].first, queries[queryIndex].second, &counter);
		}
		if (hitsCountPtr) {
			*hitsCountPtr = counter.GetCount();
		}
		return GetElapsedInSeconds(startTime, GetTime());
	}
private:
	TSharedPtr<TIntervalIndex<TIntervalBorder, TValue> > IntervalIndexPtr;
};



class TIntervalTreeWrapper : public TWrapper {
public:
	TIntervalTreeWrapper(const string id) : TWrapper(id)
										  , ContainerPtr(NULL) {
	}
	virtual void Build(const vector<TKeyId>& data) {
		vector<Interval<TValue, TIntervalBorder> > points4tree;
		for (int pointIndex = 0; pointIndex < data.size(); ++pointIndex) {
			const TIntervalBorder& start = data[pointIndex].first.first;
			const TIntervalBorder& end = data[pointIndex].first.second;
			const TValue& value = data[pointIndex].second;
			points4tree.push_back(Interval<TValue, TIntervalBorder>(start, end, data[pointIndex].second));
		}
		ContainerPtr = TSharedPtr<IntervalTree<TValue, TIntervalBorder> >
		                    (new IntervalTree<TValue, TIntervalBorder>(points4tree));
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
				std::cout << "fuckup: " << inside.size() << "\t" << hitsCount << "\n";
			}

		}


	}


	virtual double CalcQueryTime(const vector<TInterval>& queries, long long* hitsCountPtr=NULL) const {
		TTime startTime = GetTime();
		long long totalHitsCount  = 0;
		for (int query_index = 0; query_index < queries.size(); ++query_index) {
			int hitsCount = ContainerPtr->findOverlapping(queries[query_index].first, queries[query_index].second);
			totalHitsCount += hitsCount;
		}
		if (hitsCountPtr) {
			*hitsCountPtr = totalHitsCount;
		}
		return GetElapsedInSeconds(startTime, GetTime());
	}

private:
	TSharedPtr<IntervalTree<TValue, TIntervalBorder> > ContainerPtr;
};



bool RTreeCallback(int id, void* args) {
	return true;
}


class TRTreeWrapper : public TWrapper {
public:
	typedef RTree<int, double, 1, double> TRTree;
	TRTreeWrapper(const string id) : TWrapper(id), ContainerPtr(NULL) {
	}
	virtual void Build(const vector<TKeyId>& data) {
		ContainerPtr = TSharedPtr<TRTree>(new TRTree);
		for (int pointIndex = 0; pointIndex < data.size(); ++pointIndex) {
			double start[1] = { data[pointIndex].first.first };
			double end[1] = { data[pointIndex].first.second };
			ContainerPtr->Insert(start, end, data[pointIndex].second);
		}
	}
	virtual void Clear() {
		ContainerPtr = NULL;
	}

	virtual double CalcQueryTime(const vector<TInterval>& queries, long long* hitsCountPtr=NULL) const {
		TTime startTime = GetTime();
		long long totalHitsCount = 0;
		for (int queryIndex = 0; queryIndex < queries.size(); ++queryIndex) {
			double start[1] = { queries[queryIndex].first};
			double end[1] = { queries[queryIndex].second};
			int hitsCount = ContainerPtr->Search(start, end, RTreeCallback, NULL);
			totalHitsCount += hitsCount;
		}
		if (hitsCountPtr) {
			*hitsCountPtr = totalHitsCount;
		}
		return GetElapsedInSeconds(startTime, GetTime());
	}

private:
	TSharedPtr<TRTree> ContainerPtr;

};

