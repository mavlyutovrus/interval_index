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


using std::vector;
using std::string;
using std::cout;
using std::ifstream;
using std::pair;
using std::set;






template <class TIntervalBorder, class TValue>
class TIntervalIndex {
public:
	typedef std::pair<TIntervalBorder, TIntervalBorder> TInterval;
	typedef std::pair<TInterval, TValue> TKeyId;
	~TIntervalIndex() {
	}
	TIntervalIndex(const vector<TKeyId>& data, const double spaceFactor=1.0)
													: CheckpointInterval(1)
													, Index(data) {


		std::sort(Index.begin(), Index.end());
		BoundingInterval.first = Index.size() ? Index[0].first.first : 0;
		BoundingInterval.second = BoundingInterval.first;


		{//optimal chi
			vector<TIntervalBorder> inside;
			vector<int> overlappings;
			double avgOverlapping = 0;
			for (int intervalIndex = 0; intervalIndex < data.size(); ++intervalIndex) {
				if (BoundingInterval.second < data[intervalIndex].first.second) {
					BoundingInterval.second = data[intervalIndex].first.second;
				}
				{
					TIntervalBorder removeAllBiggerThan = -data[intervalIndex].first.first;
					while (inside.size()) {
						if (inside.front() <= removeAllBiggerThan) {
							break;
						}
						std::pop_heap(inside.begin(), inside.end());
						inside.pop_back();
					}
				}
				overlappings.push_back(inside.size());
				avgOverlapping += inside.size();
				inside.push_back(-data.at(intervalIndex).first.second);
				std::push_heap(inside.begin(), inside.end());
			}
			avgOverlapping = avgOverlapping / overlappings.size();
			double maxMemoryUsage = spaceFactor * data.size();
			for (CheckpointInterval = 1; CheckpointInterval <= data.size(); ++CheckpointInterval) {
				int offset = 0;
				long long memoryUsage = 0;
				while (offset < data.size()) {
					memoryUsage += overlappings[offset];
					offset += CheckpointInterval;
				}
				if (memoryUsage <= maxMemoryUsage) {
					break;
				}
			}
		}
		{//fill checkpoint arrays
			vector<TKeyId> inside;
			for (int pointIndex = 0; pointIndex < Index.size(); ++pointIndex) {
				if (pointIndex % CheckpointInterval == 0) {
					{
						vector<TKeyId> filtered;
						for (int index = 0; index < inside.size(); ++index) {
							if (inside[index].first.second >= Index[pointIndex].first.first) {
								filtered.push_back(inside[index]);
							}
						}
						std::swap(filtered, inside);
					}
					std::sort(inside.begin(), inside.end(), byRightBorderReversed);
					Checkpoints.push_back(inside);
				}
				inside.push_back(Index[pointIndex]);
			}
		}
	}
	template <class TCallback>
	void Search(const TIntervalBorder start, const TIntervalBorder stop, TCallback* callbackPtr=NULL) const {
		if (stop < start) {
			return;
		}
		if (!Index.size()) {
			return;
		}
		if (BoundingInterval.first > stop || BoundingInterval.second < start) {
			return;
		}
		int leftmostStartInsideQuery;
		if (Index.rbegin()->first.first < start) {
			leftmostStartInsideQuery = Index.size();
		} else if (Index.begin()->first.first >= start) {
			leftmostStartInsideQuery = 0;
		} else {//binary search
			int left = 0;
			int right = Index.size() - 1;
			while (right > left + 1) {
				int middle = (left + right) / 2;
				if (Index[middle].first.first < start) {
					left = middle;
				} else {
					right = middle;
				}
			}
			leftmostStartInsideQuery = left + 1;
		}
		int checkpointIndex = 0;
		if (leftmostStartInsideQuery == 0 || leftmostStartInsideQuery % CheckpointInterval > 0
										  || start == Index[leftmostStartInsideQuery].first.first) {
			checkpointIndex = leftmostStartInsideQuery / CheckpointInterval;
		} else {
			// because there might be an interval which overlaps query,
			//   but ends BEFORE leftmostStartInsideQuery's start
			checkpointIndex = (leftmostStartInsideQuery - 1) / CheckpointInterval;
		}
		checkpointIndex = std::min(checkpointIndex, (int)Checkpoints.size() - 1);
		const int checkpointPosition = CheckpointInterval * checkpointIndex;
		{//checkpoint intervals
			for (auto it = Checkpoints[checkpointIndex].begin();
					  it != Checkpoints[checkpointIndex].end(); ++it) {
				if (it->first.second >= start) {
					if (callbackPtr) {
						(*callbackPtr)(it->first, it->second);
					}
				} else {
					break; // they are sorted by right border
				}
			}
		}
		{//checkpoint_position -> start_point
			for (int current = checkpointPosition; current < leftmostStartInsideQuery; ++current) {
				if (Index[current].first.second >= start) {
					if (callbackPtr) {
						(*callbackPtr)(Index[current].first, Index[current].second);
					}
				}
			}

		}
		{//start_point -> (till left border goes beyond stop value)
			for (int current = leftmostStartInsideQuery;
					 current < Index.size() && Index[current].first.first <= stop; ++current) {
				if (callbackPtr) {
					(*callbackPtr)(Index[current].first, Index[current].second);
				}
			}
		}
	}

private:
	int CheckpointInterval;
	struct TIndexPoint {
		TIndexPoint(int borderValue, int idValue) : BorderValue(borderValue),
													IdValue(idValue) {
		}
		int BorderValue;
		int IdValue;
	};

	static bool byRightBorderReversed(const TKeyId& first, const TKeyId& second) {
		return first.first.second > second.first.second;
	}
	vector<TKeyId> Index;
	vector<vector<TKeyId> > Checkpoints;
	TInterval BoundingInterval;

};





