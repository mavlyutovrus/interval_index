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


#include "wrappers.hpp"




using std::vector;
using std::string;
using std::cout;
using std::ifstream;
using std::pair;
using std::set;




void UploadData(const char* filename, vector<TKeyId>* intervalsPtr, vector<TInterval>* queriesPtr) {
	std::ifstream stream (filename, std::ifstream::binary);
	if (!stream) {
		return;
	}
	int intervalsCount;
	stream >> intervalsCount;
	for (int interval_index = 0; interval_index < intervalsCount; ++interval_index) {
		TIntervalBorder start, end;
		TValue id;
		stream >> start >> end >> id;
		intervalsPtr->push_back(TKeyId(TInterval(start, end), id));
	}
	int queriesCount;
	stream >> queriesCount;
	for (int queryIndex = 0; queryIndex < queriesCount; ++queryIndex) {
		TIntervalBorder start, end;
		stream >> start >> end;
		queriesPtr->push_back(TInterval(start, end));
	}
}



int main(int argc, char *argv[]) {
	//initialise PAPI
	if (PAPI_library_init(PAPI_VER_CURRENT) != PAPI_VER_CURRENT) {
		exit(1);
	}

	string datasetPath = argv[1];
	vector<TKeyId> data;
	vector<TInterval> queries;
	UploadData(datasetPath.c_str(), &data, &queries);

	vector<std::shared_ptr<TWrapper> > wrappers;
	wrappers.push_back(std::shared_ptr<TWrapper>(new TRTreeWrapper("NClist")));
	wrappers.push_back(std::shared_ptr<TWrapper>(new TIntervalIndexWrapper("MavlyutovIndex")));
	wrappers.push_back(std::shared_ptr<TWrapper>(new TIntervalTreeWrapper("Interval Tree")));
	wrappers.push_back(std::shared_ptr<TWrapper>(new TRTreeWrapper("R-Tree")));
	wrappers.push_back(std::shared_ptr<TWrapper>(new TRTreeWrapper("R*-Tree")));
	wrappers.push_back(std::shared_ptr<TWrapper>(new TSegementTreeWrapper("Segment Tree")));
	long long totalHitsCount = 0;
	for (auto wrapperIt = wrappers.begin(); wrapperIt != wrappers.end(); ++wrapperIt) {
		wrapperIt->get()->Build(data);
		//wrapperIt->get()->TestQuality(data, queries);
		long long hitsCount = 0;
		double queryTime = wrapperIt->get()->CalcQueryTime(queries, &hitsCount);
		std::cout << datasetPath << "\t" << wrapperIt->get()->Id << "\t" << hitsCount << "\t" << queryTime << "\t" << wrapperIt->get()->DeltaVirtualMicroSec / 1000000.0 << "\n";
		std::cout.flush();
		wrapperIt->get()->Clear();
		if (wrapperIt == wrappers.begin()) {
			totalHitsCount = hitsCount;
		} else if (totalHitsCount != hitsCount) {
			std::cerr << "Mismatched number of results: " << totalHitsCount << " >< " <<  hitsCount << "\n";
			//exit(1);
		}
	}


	return 0;
}
