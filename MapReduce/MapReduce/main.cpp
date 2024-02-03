//
//  MapReduce
//
//  From https://en.wikipedia.org/wiki/MapReduce we can see that MapReduce is a well known
//  programming model for processing big data sets by utilizing a parallel and distributed
//  algorithm on a cluster.  MapReduce programs are composed of a map function which performs
//  sorting, filtering, or both, and a reduce function that performs a summary operation.
//
//  In this program we implement a MapReduce model to perform word counting so that we can
//  learn more about the MapReduce model.
//
//  Our program uses threads and mutexes to allow for parallel processing.  We first create a
//  map function that tokenizes the input string and then transforms the provided input string to
//  lower case with all of the punctuation marks removed.  This function stores the transformed
//  data into a vector of pairs that houses word occurences.  Our next function is the reduce function
//  which will sum the values that are contained in a provided vector.  We implement a map_worker function
//  that applies the map function to the provided input string and utilizes a mutex to help safeguard
//  updates to a intermediate step map.  The program utilizes threads to perform these executions
//  in parallel on different strings - once the threads have finished with their executions we then
//  apply the reduce phase of the model and display the word count results.
//
//

// Include libraries used for our MapReduce program
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <thread>
#include <mutex>
#include <numeric>

// Create mutual exclusion primitive
std::mutex reduceMutex;

// **************************************************************************************
//
// Function: map_function
// Description: The map function will tokenize the input string and then
//              transform the provided input string to lower case letters
//              with all punctuation marks removed.  The function will store
//              the transformed data in a vector of pairs that holds
//              word occurences.
//
// Parameters:
//   - input: This is the input string to be processed.

// Returns:
//   Vector of pairs showing word occurences in a provided text.  Each pair
//   will contain a word and the number of times it appears in the provided text.
//
// **************************************************************************************

std::vector<std::pair<std::string, int>> map_function(const std::string &input) {
    // Vectore to store word occurences - std::pair allows us to store two elements
    // that may be of different type - in this case we use type string for the word
    // in the pair and type int for the number of times the word occurs in the text
    std::vector<std::pair<std::string, int>> result;
    
    // Tokenize the input string and create a string variable called word
    std::istringstream iss(input);
    std::string word;
    
    // Process the words that have been provided in the input
    while (iss >> word) {
        // Erase any punctuation marks in the provided text
        word.erase(std::remove_if(word.begin(), word.end(), ispunct), word.end());
        // Convert all of the text to lowercase
        std::transform(word.begin(), word.end(), word.begin(),
            [](unsigned char c){ return std::tolower(c); });
        // Add the word to the result vector of pairs defined above with an
        // intitial count of one
        result.push_back(make_pair(word, 1));
    }
    // Return the vector of word occurrences
    return result;
}

// **************************************************************************************
//
// Function: reduce_function
// Description: Compute the sum of the elements in a vector
//              This function will take a vector of integers as input and
//              will compute the sum of the values.
//
// Parameters:
//   - values: The vector of integers to be summed.

// Returns:
//   Function returns the sum of all the elements in the provided vector.
//
// Note:
//   The function uses std::accumulate from the <numeric> header - if the
//   input vector is empty then the result will be 0.
//
// **************************************************************************************

int reduce_function(const std::vector<int> &values) {
    return std::accumulate(values.begin(), values.end(), 0);
}

// **************************************************************************************
//
// Function: map_worker
// Description: The map_worker function takes the provided string as input and applies
//              the map_function to it.  For each pair in the mapped_result the map_worker
//              utilizes an intermediate result map to push the result into the
//              the corresponding key.  The map_worker uses a lock_guard with the mutex
//              so that it can safely update the intermediate result map without another
//              thread attempting to update it at the same time.
//
// Parameters:
//   - input_data: String vector of input data
//   - intermediate_result: Helper map that stores intermediate results from the map phase
//   - start: Start index of the input_data
//   - end: End index of the input_data
//
// **************************************************************************************

void map_worker(const std::vector<std::string> &input_data, std::map<std::string, std::vector<int>> &intermediate_result, int start, int end) {
    for (int i = start; i < end; ++i) {
        // Apply the map function to each of the strings provided in the input data
        auto mapped_result = map_function(input_data[i]);

        // Before updating the helper map (intermediate_result) use lock_guard the the mutex
        // defined earlier to safely update the map without multiple threads updating it
        // at the same time
        for (const auto& pair : mapped_result) {
            std::lock_guard<std::mutex> lock(reduceMutex);
            intermediate_result[pair.first].push_back(pair.second);
        }
    }
}


int main(int argc, const char * argv[]) {
    // Test input sentences to show MapReduce model.
    std::vector<std::string> input_data = {
        "This is sentence one.",
        "This is sentence two.",
        "This is a sentence that ends with red.",
        "This is a sentence that ends with blue."
    };

    // Determine the number of concurrent threads that the hardware can support
    const int num_threads = std::thread::hardware_concurrency();
    
    // Create a vector of threads called map_threads
    std::vector<std::thread> map_threads;
    // Create a helper map that holds string keys and integer values
    std::map<std::string, std::vector<int>> intermediate_result;

    // The below loop will distribute the work of the MapReduce model amongst multiple threads
    // by dividing the input data into equal portions based on the number of threads determined
    // above by std::thread::hardware_concurrency.
    
    // The start and end variables will determine the range of the input data that each of the
    // threads will process.  A new thread will be created for each portion of the data and then
    // the map_worker function will be used with it with the specified parameters.
    
    // std::ref passes references to the input_data and intermediate_result to each of the threads.
    for (int i = 0; i < num_threads; ++i) {
        // Calculate the start and end indices for the current thread's portion of the provided input data
        unsigned long start = i * (input_data.size() / num_threads); // start index
        unsigned long end = (i == num_threads - 1) ? input_data.size() : (i + 1) * (input_data.size() / num_threads); // end index

        // Create new thread and assign map_worker to it - emplace_back is used to add a new thread to the end
        // of the map_threads vector - it constructs a new object in place at the end of the vector so that
        // it avoids unnecessary copying
        map_threads.emplace_back(map_worker, std::ref(input_data), std::ref(intermediate_result), start, end);
    }

    // Wait for each of the threads in map_threads to finish before proceeding
    // thread.join waits for a thread to finish its execution so we are waiting for
    // all of the threads in map_threads to finish before we move to the reduce phase
    for (auto& thread : map_threads) {
        thread.join();
    }

    // Create a map of string keys with integer values called final_result
    std::map<std::string, int> final_result;

    // Iterate over the intermediate_result map and for each of the key value
    // pairs in the map we want to apply the reduce_function to it - this is
    // the program's reduce step of the MapReduce model
    for (const auto& entry : intermediate_result) {
        final_result[entry.first] = reduce_function(entry.second);
    }

    // Iterate over the final results and print out the result set.
    for (const auto& result : final_result) {
        std::cout << result.first << ": " << result.second << std::endl;
    }

    return 0;
}

