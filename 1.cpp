#include<bits/stdc++.h>

using namespace std;


const int BUFFER_SIZE = 100;//缓冲区大小
const int BLOCK_SIZE = 10;//块大小
const int MAX_BLOCK_NUM = 100;//最大块数
const int MAX_DISK_SIZE = 1000;//最大磁盘空间
const string TMP_FILE_PREFIX = "tmp";//临时文件前缀

/* 流程
 * 从磁盘中读取一个子集合，排序后得到顺序的块，依次将块装入内存，取第一个元素排序，形成一个待比较集合
 * 将待比较集合中的最小元素放入输出块，取所在块的下一个元素放入待比较集合，输出块满后输出到磁盘中
 * */

//元组
//struct Tuple{
//    int data;//数据
//    int block_id;//块编号
//    Tuple(int d = 0, int b = -1): data(d), block_id(b) {}
//};

//块
struct Block {
    vector<int> tuples;//元组
    int id;//块编号
    bool operator<(const Block &b) const {
        return tuples[0] < b.tuples[0];
    }
};

//缓冲区(内存)
struct Buffer {
    vector<Block> blocks;//块
    int read_ptr;//读指针
    int write_ptr;//写指针
    Buffer() : read_ptr(0), write_ptr(0) {}
};

//磁盘
struct Disk {
    vector<Block> blocks;//块
    int size;//磁盘大小
    Disk() : size(0) {}

    int WriteBlock(Block &b) {//写块到磁盘
        if (size + 1 <= MAX_DISK_SIZE) {
            b.id = size;
            blocks.push_back(b);
            size++;
            return b.id;
        }
        return -1;
    }

    void ReadBlock(int id, Block &b) {//从磁盘读块
        if (id >= 0 && id < size) {
            b = blocks[id];
        }
    }
};

//文件
struct File {
    vector<string> filenames;//文件名
    int file_num;//文件数量
    File() : file_num(0) {}

    string GenerateFilename() {//生成新文件名
        file_num++;
        return TMP_FILE_PREFIX + to_string(file_num) + ".txt";
    }

    void RemoveFile(string filename) {//删除文件
        remove(filename.c_str());
    }

    void MergeFiles(string filename1, string filename2, string output) {//归并文件
        ifstream input1(filename1);
        ifstream input2(filename2);
        ofstream output_file(output);
        int num1, num2;
        input1 >> num1;
        input2 >> num2;
        while (input1 && input2) {
            if (num1 < num2) {
                output_file << num1 << endl;
                input1 >> num1;
            } else {
                output_file << num2 << endl;
                input2 >> num2;
            }
        }
        while (input1) {
            output_file << num1 << endl;
            input1 >> num1;
        }
        while (input2) {
            output_file << num2 << endl;
            input2 >> num2;
        }
        input1.close();
        input2.close();
        output_file.close();
        RemoveFile(filename1);
        RemoveFile(filename2);
    }
};


//将一个块写入文件中
void SaveBlock(const string &filename, int block_id, const Block &block) {
    fstream file(filename, ios::binary | ios::in | ios::out);
    file.seekp(block_id * sizeof(Block), ios::beg);
    file.write((char *) &block, sizeof(Block));
    file.close();
}

//将一个文件中的所有块打印出来
void PrintFile(const string &filename) {
    Block block;
    ifstream file(filename, ios::binary);
    while (!file.eof()) {
        file.read((char *) &block, sizeof(Block));
        if (!file.eof()) {
            for (int i = 0; i < block.tuples.size(); i++) {
                cout << block.tuples[i] << " ";
            }
        }
        cout << endl;
    }
    file.close();
}

//上述代码使用了随机生成数据的函数GenerateRandomBlock，代码如下所示：

void GenerateRandomBlock(const string &filename, int min_val, int max_val, int num_tuples) {
    Block block;
//    block.tuples.size() = num_tuples;
    for (int i = 0; i < num_tuples; i++) {
        block.tuples[i] = rand() % (max_val - min_val + 1) + min_val;
    }
    SaveBlock(filename, 0, block);
}


//将元组写入块中
void WriteTupleToBlock(int tuple, Block &block) {
    if (block.tuples.size() >= BLOCK_SIZE) {
        cerr << "错误: 这块满了！" << endl;
        return;
    }
    block.tuples.push_back(tuple);
}

//从块中读取元组
void ReadTupleFromBlock(int &tuple, Block &block, int pos) {
    if (pos < 0 || pos >= block.tuples.size()) {
        cerr << "错误：位置无效！" << endl;
        return;
    }
    tuple = block.tuples[pos];
}

//从磁盘中读取块
void ReadBlockFromDisk(Block &block, Disk &disk, int block_id) {
    if (block_id < 0 || block_id >= disk.size) {
        cerr << "错误：无效的块ID." << endl;
        return;
    }
    disk.ReadBlock(block_id, block);
}

//将块写入磁盘
void WriteBlockToDisk(Block &block, Disk &disk) {
    if (disk.WriteBlock(block) == -1) {
        cerr << "错误：磁盘满了！" << endl;
        return;
    }
}

//将缓冲区写入磁盘
void WriteBufferToDisk(Buffer &buffer, Disk &disk) {
    for (int i = 0; i < buffer.blocks.size(); i++) {
        WriteBlockToDisk(buffer.blocks[i], disk);
    }
    buffer.blocks.clear();
    buffer.write_ptr = 0;
}

//从磁盘中读取块到缓冲区
void ReadBlockToBuffer(Buffer &buffer, Disk &disk, int block_id) {
    if (buffer.blocks.size() >= BUFFER_SIZE) {
        cerr << "Error: Buffer is full" << endl;
        return;
    }
    Block block;
    ReadBlockFromDisk(block, disk, block_id);
    buffer.blocks.push_back(block);
}

//将缓冲区写入文件
void WriteBufferToFile(Buffer &buffer, File &file) {
    string filename = file.GenerateFilename();
    ofstream output(filename);
    for (int i = 0; i < buffer.blocks.size(); i++) {
        for (int j = 0; j < buffer.blocks[i].tuples.size(); j++) {
            output << buffer.blocks[i].tuples[j] << endl;
        }
    }
    output.close();
    file.filenames.push_back(filename);
    buffer.blocks.clear();
    buffer.write_ptr = 0;
}

//从文件读取块到缓冲区
void ReadFileToBuffer(Buffer &buffer, File &file, int block_id) {
    if (block_id < 0 || block_id >= file.filenames.size()) {
        cerr << "Error: Invalid block ID" << endl;
        return;
    }
    string filename = file.filenames[block_id];
    ifstream input(filename);
    if (!input) {
        cerr << "Error: Failed to open file " << filename << endl;
        return;
    }
    Block block;
    int cnt = 0;
    while (input >> block.tuples[cnt]) {
        block.id = block_id;
        cnt++;
        if (cnt >= BLOCK_SIZE) {
            buffer.blocks.push_back(block);
            cnt = 0;
        }
    }
    if (cnt > 0) {
        buffer.blocks.push_back(block);
    }
    input.close();
}

//对块中的元组进行排序
void SortBlock(Block &block) {
    sort(block.tuples.begin(), block.tuples.end());
}

//对缓冲区中的所有块进行排序
void SortBuffer(Buffer &buffer) {
    for (int i = 0; i < buffer.blocks.size(); i++) {
        SortBlock(buffer.blocks[i]);
    }
}

//对已排序的两个块进行归并操作
void MergeBlocks(Block &block1, Block &block2, Block &result) {
    int i = 0, j = 0;
    while (i < block1.tuples.size() && j < block2.tuples.size()) {
        if (block1.tuples[i] < block2.tuples[j]) {
            WriteTupleToBlock(block1.tuples[i], result);
            i++;
        } else {
            WriteTupleToBlock(block2.tuples[j], result);
            j++;
        }
    }
    while (i < block1.tuples.size()) {
        WriteTupleToBlock(block1.tuples[i], result);
        i++;
    }
    while (j < block2.tuples.size()) {
        WriteTupleToBlock(block2.tuples[j], result);
        j++;
    }
}

//对已排序的多个块进行归并操作
void MergeSortedBlocks(vector<Block> &blocks, Block &result) {
    int n = blocks.size();
    if (n == 1) {
        result = blocks[0];
        return;
    }
    vector<int> ptrs(n, 0);
    while (true) {
        int min_pos = -1;
        for (int i = 0; i < n; i++) {
            if (ptrs[i] < blocks[i].tuples.size()) {
                if (min_pos == -1 || blocks[i].tuples[ptrs[i]] < blocks[min_pos].tuples[ptrs[min_pos]]) {
                    min_pos = i;
                }
            }
        }
        if (min_pos == -1) {
            break;
        }
        WriteTupleToBlock(blocks[min_pos].tuples[ptrs[min_pos]], result);
        ptrs[min_pos]++;
    }
}

//对块进行去重复操作
void DeduplicateBlock(Block &block) {
    vector<int> tuples;
    set<int> s;
    for (int i = 0; i < block.tuples.size(); i++) {
        if (s.find(block.tuples[i]) == s.end()) {
            tuples.push_back(block.tuples[i]);
            s.insert(block.tuples[i]);
        }
    }
    block.tuples = tuples;
}

//对缓冲区中的所有块进行去重复操作
void DeduplicateBuffer(Buffer &buffer) {
    for (int i = 0; i < buffer.blocks.size(); i++) {
        DeduplicateBlock(buffer.blocks[i]);
    }
}

//对已排序的两个块进行集合交操作
void IntersectBlocks(Block &block1, Block &block2, Block &result) {
    int i = 0, j = 0;
    while (i < block1.tuples.size() && j < block2.tuples.size()) {
        if (block1.tuples[i] < block2.tuples[j]) {
            i++;
        } else if (block2.tuples[j] < block1.tuples[i]) {
            j++;
        } else {
            WriteTupleToBlock(block1.tuples[i], result);
            i++;
            j++;
        }
    }
}

//对已排序的两个块进行集合并操作
void UnionBlocks(Block &block1, Block &block2, Block &result) {
    int i = 0, j = 0;
    while (i < block1.tuples.size() && j < block2.tuples.size()) {
        if (block1.tuples[i] < block2.tuples[j]) {
            WriteTupleToBlock(block1.tuples[i], result);
            i++;
        } else if (block2.tuples[j] < block1.tuples[i]) {
            WriteTupleToBlock(block2.tuples[j], result);
            j++;
        } else {
            WriteTupleToBlock(block1.tuples[i], result);
            i++;
            j++;
        }
    }
    while (i < block1.tuples.size()) {
        WriteTupleToBlock(block1.tuples[i], result);
        i++;
    }
    while (j < block2.tuples.size()) {
        WriteTupleToBlock(block2.tuples[j], result);
        j++;
    }
}

//对已排序的两个块进行集合差操作
void SubtractBlocks(Block &block1, Block &block2, Block &result) {
    int i = 0, j = 0;
    while (i < block1.tuples.size() && j < block2.tuples.size()) {
        if (block1.tuples[i] < block2.tuples[j]) {
            WriteTupleToBlock(block1.tuples[i], result);
            i++;
        } else if (block2.tuples[j] < block1.tuples[i]) {
            j++;
        } else {
            i++;
            j++;
        }
    }
    while (i < block1.tuples.size()) {
        WriteTupleToBlock(block1.tuples[i], result);
        i++;
    }
}

//对多个块进行集合交操作
void IntersectSortedBlocks(vector<Block> &blocks, Block &result) {
    int n = blocks.size();
    if (n == 1) {
        result = blocks[0];
        return;
    }
    vector<int> ptrs(n, 0);
    while (true) {
        bool flag = true;
        for (int i = 0; i < n; i++) {
            if (ptrs[i] >= blocks[i].tuples.size() || blocks[i].tuples[ptrs[i]] > blocks[0].tuples[ptrs[0]]) {
                flag = false;
                break;
            }
        }
        if (!flag) {
            break;
        }
        bool eq = true;
        for (int i = 1; i < n; i++) {
            if (blocks[i].tuples[ptrs[i]] < blocks[0].tuples[ptrs[0]]) {
                ptrs[i]++;
                eq = false;
                break;
            }
            if (blocks[i].tuples[ptrs[i]] > blocks[0].tuples[ptrs[0]]) {
                ptrs[0]++;
                eq = false;
                break;
            }
        }
        if (eq) {
            WriteTupleToBlock(blocks[0].tuples[ptrs[0]], result);
            for (int i = 0; i < n; i++) {
                ptrs[i]++;
            }
        }
    }
}

//对多个块进行集合并操作
void UnionSortedBlocks(vector<Block> &blocks, Block &result) {
    int n = blocks.size();
    if (n == 1) {
        result = blocks[0];
        return;
    }
    vector<int> ptrs(n, 0);
    while (true) {
        int min_pos = -1;
        for (int i = 0; i < n; i++) {
            if (ptrs[i] < blocks[i].tuples.size()) {
                if (min_pos == -1 || blocks[i].tuples[ptrs[i]] < blocks[min_pos].tuples[ptrs[min_pos]]) {
                    min_pos = i;
                }
            }
        }
        if (min_pos == -1) {
            break;
        }
        WriteTupleToBlock(blocks[min_pos].tuples[ptrs[min_pos]], result);
        ptrs[min_pos]++;
    }

}

//对一个块进行去重操作
void RemoveDuplicate(Block &block) {
    if (block.tuples.empty()) {
        return;
    }
    int j = 0;
    for (int i = 1; i < block.tuples.size(); i++) {
        if (block.tuples[i] != block.tuples[j]) {
            j++;
            block.tuples[j] = block.tuples[i];
        }
    }
    block.tuples.erase(block.tuples.begin() + j + 1, block.tuples.end());
}

//将块中元组按照给定的属性排序
void SortBlock(Block &block, int sort_attr) {
    sort(block.tuples.begin(), block.tuples.end(), [=](const int &t1, const int &t2) {
        return t1[sort_attr] < t2[sort_attr];
    });
}

//将输入文件中的所有元组按照给定的属性排序
void SortFile(string input_file, string output_file, int sort_attr, int mem_size) {
    int block_size = (mem_size * 1024) / sizeof(int);
    int n_blocks = GetNumBlocks(input_file, block_size);
    vector<Block> blocks(n_blocks);
    for (int i = 0; i < n_blocks; i++) {
        LoadBlock(input_file, i, block_size, blocks[i]);
        SortBlock(blocks[i], sort_attr);
    }
    vector<queue<Block>> queues(2);
    for (int i = 0; i < n_blocks; i++) {
        queues[0].push(blocks[i]);
    }
    int ptr = 0;
    while (queues[ptr % 2].size() > 1) {
        int n_merge_blocks = min(mem_size / 2, (int) queues[ptr % 2].size());
        vector<Block> merge_blocks(n_merge_blocks);
        for (int i = 0; i < n_merge_blocks; i++) {
            merge_blocks[i] = queues[ptr % 2].front();
            queues[ptr % 2].pop();
        }
        Block result;
        MergeSortedBlocks(merge_blocks, result);
        queues[(ptr + 1) % 2].push(result);
    }
    Block sorted_block;
    sorted_block = queues[ptr % 2].front();
    queues[ptr % 2].pop();
    while (!queues[ptr % 2].empty()) {
        Block block;
        block = queues[ptr % 2].front();
        queues[ptr % 2].pop();
        Block result;
        MergeSortedBlocks({sorted_block, block}, result);
        sorted_block = result;
    }
    RemoveDuplicate(sorted_block);
    SaveBlock(output_file, 0, sorted_block);
}

//将输入文件中的所有元组去重
void RemoveDuplicates(string input_file, string output_file, int mem_size) {
    int block_size = (mem_size * 1024) / sizeof(int);
    int n_blocks = GetNumBlocks(input_file, block_size);
    vector<Block> blocks(n_blocks);
    for (int i = 0; i < n_blocks; i++) {
        LoadBlock(input_file, i, block_size, blocks[i]);
        RemoveDuplicate(blocks[i]);
    }
    vector<queue<Block>> queues(2);
    for (int i = 0; i < n_blocks; i++) {
        queues[0].push(blocks[i]);
    }
    int ptr = 0;
    while (queues[ptr % 2].size() > 1) {
        int n_merge_blocks = min(mem_size / 2, (int) queues[ptr % 2].size());
        vector<Block> merge_blocks(n_merge_blocks);
        for (int i = 0; i < n_merge_blocks; i++) {
            merge_blocks[i] = queues[ptr % 2].front();
            queues[ptr % 2].pop();
        }
        Block result;
        MergeSortedBlocks(merge_blocks, result);
        queues[(ptr + 1) % 2].push(result);
    }
    Block deduped_block;
    deduped_block = queues[ptr % 2].front();
    queues[ptr % 2].pop();
    while (!queues[ptr % 2].empty()) {
        Block block;
        block = queues[ptr % 2].front();
        queues[ptr % 2].pop();
        Block result;
        MergeSortedBlocks({deduped_block, block}, result);
        deduped_block = result;
    }
    SaveBlock(output_file, 0, deduped_block);
}

//求两个有序块的并集
void UnionSortedBlocks(const Block &block1, const Block &block2, Block &result) {
    result.tuples.clear();
    int i = 0, j = 0;
    while (i < block1.tuples.size() && j < block2.tuples.size()) {
        if (block1.tuples[i] == block2.tuples[j]) {
            result.tuples.push_back(block1.tuples[i]);
            i++;
            j++;
        } else if (block1.tuples[i] < block2.tuples[j]) {
            result.tuples.push_back(block1.tuples[i]);
            i++;
        } else {
            result.tuples.push_back(block2.tuples[j]);
            j++;
        }
    }
    while (i < block1.tuples.size()) {
        result.tuples.push_back(block1.tuples[i]);
        i++;
    }
    while (j < block2.tuples.size()) {
        result.tuples.push_back(block2.tuples[j]);
        j++;
    }
}

//求两个有序块的交集
void IntersectSortedBlocks(const Block &block1, const Block &block2, Block &result) {
    result.tuples.clear();
    int i = 0, j = 0;
    while (i < block1.tuples.size() && j < block2.tuples.size()) {
        if (block1.tuples[i] == block2.tuples[j]) {
            result.tuples.push_back(block1.tuples[i]);
            i++;
            j++;
        } else if (block1.tuples[i] < block2.tuples[j]) {
            i++;
        } else {
            j++;
        }
    }
}

//求两个有序块的差集
void MinusSortedBlocks(const Block &block1, const Block &block2, Block &result) {
    result.tuples.clear();
    int i = 0, j = 0;
    while (i < block1.tuples.size() && j < block2.tuples.size()) {
        if (block1.tuples[i] == block2.tuples[j]) {
            i++;
            j++;
        } else if (block1.tuples[i] < block2.tuples[j]) {
            result.tuples.push_back(block1.tuples[i]);
            i++;
        } else {
            j++;
        }
    }
    while (i < block1.tuples.size()) {
        result.tuples.push_back(block1.tuples[i]);
        i++;
    }
}

//对输入文件中的两个有序关系进行并集操作
void UnionSortedRelations(const string &input_file1, const string &input_file2, const string &output_file) {
    Block block1, block2, result;
    ifstream input1(input_file1, ios::binary), input2(input_file2, ios::binary);
    input1.read((char *) &block1, sizeof(Block));
    input2.read((char *) &block2, sizeof(Block));
    while (!input1.eof() && !input2.eof()) {
        UnionSortedBlocks(block1, block2, result);
        SaveBlock(output_file, 0, result);
        if (block1.tuples.back() <= block2.tuples.back()) {
            input1.read((char *) &block1, sizeof(Block));
        }
        if (block2.tuples.back() <= block1.tuples.back()) {
            input2.read((char *) &block2, sizeof(Block));
        }
    }
    input1.close();
    input2.close();
}

//对输入文件中的两个有序关系进行交集操作
void IntersectSortedRelations(const string &input_file1, const string &input_file2, const string &output_file) {
    Block block1, block2, result;
    ifstream input1(input_file1, ios::binary), input2(input_file2, ios::binary);
    input1.read((char *) &block1, sizeof(Block));
    input2.read((char *) &block2, sizeof(Block));
    while (!input1.eof() && !input2.eof()) {
        IntersectSortedBlocks(block1, block2, result);
        SaveBlock(output_file, 0, result);
        if (block1.tuples.back() <= block2.tuples.back()) {
            input1.read((char *) &block1, sizeof(Block));
        }
        if (block2.tuples.back() <= block1.tuples.back()) {
            input2.read((char *) &block2, sizeof(Block));
        }
    }
    input1.close();
    input2.close();
}

//对输入文件中的两个有序关系进行差集操作
void MinusSortedRelations(const string &input_file1, const string &input_file2, const string &output_file) {
    Block block1, block2, result;
    ifstream input1(input_file1, ios::binary), input2(input_file2, ios::binary);
    input1.read((char *) &block1, sizeof(Block));
    input2.read((char *) &block2, sizeof(Block));
    while (!input1.eof() && !input2.eof()) {
        MinusSortedBlocks(block1, block2, result);
        SaveBlock(output_file, 0, result);
        if (block1.tuples.back() <= block2.tuples.back()) {
            input1.read((char *) &block1, sizeof(Block));
        }
        if (block2.tuples.back() <= block1.tuples.back()) {
            input2.read((char *) &block2, sizeof(Block));
        }
    }
    while (!input1.eof()) {
        input1.read((char *) &block1, sizeof(Block));
        SaveBlock(output_file, 0, block1);
    }
    input1.close();
    input2.close();
}

int main() {
    int n_blocks = 20;
    int mem_size = 5;
    vector<string> filenames(n_blocks);
    for (int i = 0; i < n_blocks; i++) {
        filenames[i] = "block_" + to_string(i) + ".bin";
        GenerateRandomBlock(filenames[i], 0, 10, 10);
    }
    string result_file;
    result_file = "result_union.bin";
    UnionRelations(filenames, result_file, mem_size);
    PrintFile(result_file);
    result_file = "result_intersect.bin";
    IntersectRelations(filenames, result_file, mem_size);
    PrintFile(result_file);
    result_file = "result_minus.bin";
    MinusRelations(filenames[0], filenames[1], result_file);
    PrintFile(result_file);
    return 0;
}

