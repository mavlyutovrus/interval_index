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
	string datasetPath = argv[1];
	vector<TKeyId> data;
	vector<TInterval> queries;
	UploadData(datasetPath.c_str(), &data, &queries);
	{
	    vector<TKeyId> buffer;
	    for (int i = 0; i < 20; ++i) {
	        buffer.insert(buffer.end(), data.begin(), data.end());
	    }
	    std::cout << buffer.size() << "\n";
	}
	{
		TIntervalIndexTester tester("free", 1);
		tester.Build(data);
		long long dataMemConsumption, dsMemConsumption;
		tester.IntervalIndexPtr->GetMemoryConsumption(&dataMemConsumption, &dsMemConsumption);
		long long dummyCounter = 0;
		double binSearchTime, noInsideTime, fullQueryTime;
		tester.CalcQueryTime(queries, &dummyCounter, &binSearchTime, &noInsideTime, &fullQueryTime);
		std::cout << datasetPath
					  << "\t" << tester.Id
					  << "\t" << dataMemConsumption
					  << "\t" << dsMemConsumption
					  << "\t" << dummyCounter
					  << "\t" << tester.IntervalIndexPtr->GetCheckpointInterval()
					  << "\t" << binSearchTime
					  << "\t" << noInsideTime
					  << "\t" << fullQueryTime << "\n";
		std::cout.flush();
	}

	return 0;
}
