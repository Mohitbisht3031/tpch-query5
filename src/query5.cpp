#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <iomanip>

std::mutex results_mutex;
// Define expected columns for each table
// Hardcoded for simplicity, can be replaced with dynamic loading
// Here we can even remove the hardcoded table names since we are using the same table names as in the TPCH schema, but keeping it for clarity
std::vector<std::pair<std::string, std::vector<std::string>>> tables = {
    {"customer.tbl", {"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"}},
    {"orders.tbl", {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"}},
    {"lineitem.tbl", {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"}},
    {"supplier.tbl", {"s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"}},
    {"nation.tbl", {"n_nationkey", "n_name", "n_regionkey", "n_comment"}},
    {"region.tbl", {"r_regionkey", "r_name", "r_comment"}}
};

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    // Example: --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 4 --table_path /path/to/tables --result_path /path/to/results
    bool validArgs = true;
    for(int i = 1;i<argc;i+=2){
        if(std::string(argv[i]) == "--r_name") {
            if(i + 1 < argc) {
                r_name = argv[i + 1];
            } else {
                std::cerr << "Missing value for --r_name" << std::endl;
                validArgs = false;
            }
        } else if(std::string(argv[i]) == "--start_date") {
            if(i + 1 < argc) {
                start_date = argv[i + 1];
            } else {
                std::cerr << "Missing value for --start_date" << std::endl;
                validArgs = false;
            }
        } else if(std::string(argv[i]) == "--end_date") {
            if(i + 1 < argc) {
                end_date = argv[i + 1];
            } else {
                std::cerr << "Missing value for --end_date" << std::endl;
                validArgs = false;
            }
        } else if(std::string(argv[i]) == "--threads") {
            if(i + 1 < argc) {
                num_threads = std::stoi(argv[i + 1]);
            } else {
                std::cerr << "Missing value for --threads" << std::endl;
                validArgs = false;
            }
        } else if(std::string(argv[i]) == "--table_path") {
            if(i + 1 < argc) {
                table_path = argv[i + 1];
            } else {
                std::cerr << "Missing value for --table_path" << std::endl;
                validArgs = false;
            }
        } else if(std::string(argv[i]) == "--result_path") {
            if(i + 1 < argc) {
                result_path = argv[i + 1];
            } else {
                std::cerr << "Missing value for --result_path" << std::endl;
                validArgs = false;
            }
        } else {
            std::cerr << "Unknown argument: " << argv[i] << std::endl;
            return false;
        }
        if(!validArgs) {
            break;
        }
    }
    return validArgs;
}

void readData(const std::string &table_path, std::vector<std::map<std::string, std::string>>& data,int table_index) {
    std::ifstream file(table_path);
    // Check if the file opened successfully
    if (!file.is_open()) {
        std::cerr << "Error opening file: " << table_path << std::endl;
        return;
    }
    std::string line;
    while(std::getline(file, line)) {
        std::map<std::string, std::string> row;
        std::istringstream ss(line);
        std::string value;
        size_t col_index = 0;
        while (std::getline(ss, value, '|')) {
            if (col_index < tables[table_index].second.size()) { // check if column index is within bounds
                row[tables[table_index].second[col_index]] = value; // Use the specific table's column names
            }
            col_index++;
        }
        data.push_back(row);
    }
    file.close();
    return;
}

// Function to read TPCH data from the specified paths
bool readTPCHData(const std::string& table_path, std::vector<std::map<std::string, 
    std::string>>& customer_data, std::vector<std::map<std::string, 
    std::string>>& orders_data, std::vector<std::map<std::string, 
    std::string>>& lineitem_data, std::vector<std::map<std::string, 
    std::string>>& supplier_data, std::vector<std::map<std::string, 
    std::string>>& nation_data, std::vector<std::map<std::string, std::string>>& region_data) {
    
    try{
        // Read each table's data
        readData(table_path + "/customer.tbl", customer_data, 0);
        if(customer_data.size() == 0) {
            std::cerr << "Error reading customer data from: " << table_path + "/customer.tbl" << std::endl;
            return false;
        }
        readData(table_path + "/orders.tbl", orders_data, 1);
        if(orders_data.size() == 0) {
            std::cerr << "Error reading orders data from: " << table_path + "/orders.tbl" << std::endl;
            return false;
        }
        readData(table_path + "/lineitem.tbl", lineitem_data, 2);
        if(lineitem_data.size() == 0) {
            std::cerr << "Error reading lineitem data from: " << table_path + "/lineitem.tbl" << std::endl;
            return false;
        }
        readData(table_path + "/supplier.tbl", supplier_data, 3);
        if(supplier_data.size() == 0) {
            std::cerr << "Error reading supplier data from: " << table_path + "/supplier.tbl" << std::endl;
            return false;
        }
        readData(table_path + "/nation.tbl", nation_data, 4);
        if(nation_data.size() == 0) {
            std::cerr << "Error reading nation data from: " << table_path + "/nation.tbl" << std::endl;
            return false;
        }
        readData(table_path + "/region.tbl", region_data, 5);
        if(region_data.size() == 0) {
            std::cerr << "Error reading region data from: " << table_path + "/region.tbl" << std::endl;
            return false;
        }   
    }
    catch(const std::exception& e) {
        std::cerr << "Exception occurred while reading TPCH data: " << e.what() << std::endl;
        return false;
    }
   std::cout<<"Reading TPCH data from: " << table_path << std::endl;
    
    return true;
}

bool inDateRange(const std::string& d, const std::string& start, const std::string& end) {
    return d >= start && d < end;
}

// lookup helpers (linear scan, but you can optimize with unordered_map indices)
static const std::map<std::string,std::string>* findCustomer(const std::vector<std::map<std::string,std::string>>& cust,
                                                             const std::string& key) {
    for (auto& c : cust)
        if (c.at("c_custkey") == key)
            return &c;
    return nullptr;
}

static const std::map<std::string,std::string>* findSupplier(const std::vector<std::map<std::string,std::string>>& sup,
                                                             const std::string& key) {
    for (auto& s : sup)
        if (s.at("s_suppkey") == key)
            return &s;
    return nullptr;
}

static const std::map<std::string,std::string>* findNation(const std::vector<std::map<std::string,std::string>>& nat,
                                                            const std::string& key) {
    for (auto& n : nat)
        if (n.at("n_nationkey") == key)
            return &n;
    return nullptr;
}

static const std::map<std::string,std::string>* findRegion(const std::vector<std::map<std::string,std::string>>& reg,
                                                            const std::string& key) {
    for (auto& r : reg)
        if (r.at("r_regionkey") == key)
            return &r;
    return nullptr;
}

void processInfo(size_t start_index, size_t end_index,
                 const std::string& r_name,
                 const std::string& start_date,
                 const std::string& end_date,
                 const std::vector<std::map<std::string, std::string>>& customer_data,
                 const std::vector<std::map<std::string, std::string>>& orders_data,
                 const std::vector<std::map<std::string, std::string>>& lineitem_data,
                 const std::vector<std::map<std::string, std::string>>& supplier_data,
                 const std::vector<std::map<std::string, std::string>>& nation_data,
                 const std::vector<std::map<std::string, std::string>>& region_data,
                 std::map<std::string, double>& results) 
{
    std::map<std::string,double> local_result;
    std::cout << "Processing orders from index " << start_index << " to " << end_index << std::endl;
    for (size_t oi = start_index; oi < end_index; ++oi) {
        const auto& o = orders_data[oi];
        const std::string& od = o.at("o_orderdate");
        if (!inDateRange(od, start_date, end_date)) continue;

        const std::string& ckey = o.at("o_custkey");
        auto cust = findCustomer(customer_data, ckey);
        if (!cust) continue;

        // fetch nation via customer
        auto cust_nkey = cust->at("c_nationkey");
        auto cust_nat = findNation(nation_data, cust_nkey);
        if (!cust_nat) continue;

        auto reg = findRegion(region_data, cust_nat->at("n_regionkey"));
        if (!reg || reg->at("r_name") != r_name) continue;

        // Now scan lineitems matching this order
        for (auto& l : lineitem_data) {
            if (l.at("l_orderkey") != o.at("o_orderkey")) continue;

            auto supp = findSupplier(supplier_data, l.at("l_suppkey"));
            if (!supp) continue;
            if (supp->at("s_nationkey") != cust_nkey) continue;

            // supplier nation
            auto supp_nat = findNation(nation_data, supp->at("s_nationkey"));
            if (!supp_nat) continue;

            // Revenue compute: extendedprice * (1 - discount)
            double ext = std::stod(l.at("l_extendedprice"));
            double disc = std::stod(l.at("l_discount"));
            double revenue = ext * (1.0 - disc);

            const std::string& nationName = cust_nat->at("n_name");
            local_result[nationName] += revenue;
        }
    }

    // merge local_result into global results
    std::lock_guard<std::mutex> guard(results_mutex);
    for (auto& kv : local_result) {
        results[kv.first] += kv.second;
    }
}

// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads, const std::vector<std::map<std::string, std::string>>& customer_data, const std::vector<std::map<std::string, std::string>>& orders_data, const std::vector<std::map<std::string, std::string>>& lineitem_data, const std::vector<std::map<std::string, std::string>>& supplier_data, const std::vector<std::map<std::string, std::string>>& nation_data, const std::vector<std::map<std::string, std::string>>& region_data, std::map<std::string, double>& results) {
    
    results[r_name] = 0.0; // Initialize result for the specified region

    size_t order_count = orders_data.size();
    size_t orders_per_thread = order_count / num_threads;
    std::vector<std::thread> threads;
    std::cout<< "Starting query execution with " << num_threads << " threads." << std::endl;

    for(int i = 0; i < num_threads; ++i) {
        size_t start_index = i * orders_per_thread;
        size_t end_index = (i == num_threads - 1) ? order_count : start_index + orders_per_thread;

        threads.emplace_back(processInfo,start_index, end_index,
            std::ref(r_name), std::ref(start_date), std::ref(end_date),
            std::ref(customer_data), std::ref(orders_data), std::ref(lineitem_data),
            std::ref(supplier_data), std::ref(nation_data), std::ref(region_data),
            std::ref(results));
    }
    for(auto& thread : threads) {
        if(thread.joinable()) {
            thread.join();
        }else {
            std::cerr << "Thread not joinable." << std::endl;
            return false;
        }
    }
    return true;
}

// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    std::ofstream file(result_path);
    // Check if the file opened successfully
    if (!file.is_open()) {
        std::cerr << "Error opening result file: " << result_path << std::endl;
        return false;
    }

    std::vector<std::pair<std::string, double>> sorted_results(results.begin(), results.end());
    std::sort(sorted_results.begin(), sorted_results.end(), [](const std::pair<std::string, double>& a, const std::pair<std::string, double>& b) {
        return a.second > b.second; // Sort by value in descending order
    });
    try{
        file << "c_custkey|revenue\n"; // key header
        for (const auto& result : sorted_results) {
            file << result.first << "|" << std::fixed << std::setprecision(2) << result.second << "\n"; // Write each result in the format "key|value"
        }
        file.close();
        if (!file) {
            std::cerr << "Error writing to result file: " << result_path << std::endl;
            return false;
        }   
    }
    catch(const std::exception& e) {
        std::cerr << "Exception occurred while writing results: " << e.what() << std::endl;
        return false;
    }

    return true;
} 