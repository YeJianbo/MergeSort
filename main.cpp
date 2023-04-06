#include<bits/stdc++.h>

using namespace std;


const int BUFFER_SIZE = 200;//缓冲区大小
const int BLOCK_SIZE = 4;//块大小
const int MAX_BLOCK_NUM = 100;//最大块数
const int MAX_DISK_SIZE = 400;//最大磁盘空间
const int GROUP_SIZE = 2;


/* 流程
 * 从磁盘中读取一个子集合，排序后得到顺序的块，依次将块装入内存，取第一个元素排序，形成一个待比较集合
 * 将待比较集合中的最小元素放入输出块，取所在块的下一个元素放入待比较集合，输出块满后输出到磁盘中
 * */


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
    vector<Block> blocks;//输入块
    Block result;//结果块，单个块，用于临时保存结果，存满就写进磁盘然后清空
//    vector<int> ptr;//读指针
//    int write_ptr;//写指针
//    Buffer() : read_ptr(0), write_ptr(0) {}
};

//磁盘
class Disk {
    vector<Block> blocks;//存储输入数据的块
//    int size;//磁盘大小
    vector<Block> result;//保存排序后的结果

    int size = 0;//已读取进入缓冲区处理过的大小
public:
    int WriteBlock(Block &b) {//写块到磁盘
        if (blocks.size() + result.size() + 1 <= MAX_DISK_SIZE) {
            b.id = blocks.size();
            blocks.push_back(b);
            return b.id;
        }
        return -1;
    }

    int WriteResult(Block &b) {//写块到磁盘
        if (blocks.size() + result.size() + 1 <= MAX_DISK_SIZE) {
            b.id = result.size();
            result.push_back(b);
            return b.id;
        }
        return -1;
    }

    Block ReadBlock(int id) {//从磁盘读块
        if (id >= 0 && id < blocks.size()) {
            return blocks[id];
        }
        return Block();
    }

    Block ReadResult(int id) {//从磁盘读块
        if (id >= 0 && id < result.size()) {
            return result[id];
        }
        return Block();
    }

    int getBlockSize() {
        return blocks.size();
    }

    int getResultSize(){
        return result.size();
    }

    int getSize() {
        return size;
    }

    void addSize() {
        size++;
    }

    void sortBlocks() {
        int numBlocks = getBlockSize();
        int numPairs = numBlocks / 2;
        for (int i = 0; i < numPairs; i++) {
            Block b1 = ReadBlock(i * 2);
            Block b2 = ReadBlock(i * 2 + 1);
            sort(b1.tuples.begin(), b1.tuples.end());
            sort(b2.tuples.begin(), b2.tuples.end());
            Block merged;
            merged.tuples.reserve(b1.tuples.size() + b2.tuples.size());
            merge(b1.tuples.begin(), b1.tuples.end(), b2.tuples.begin(), b2.tuples.end(), back_inserter(merged.tuples));
            Block b3, b4;
            int mid = merged.tuples.size() / 2;
            b3.tuples.resize(mid);
            b4.tuples.resize(merged.tuples.size() - mid);
            std::copy(merged.tuples.begin(), merged.tuples.begin() + mid, b3.tuples.begin());
            std::copy(merged.tuples.begin() + mid, merged.tuples.end(), b4.tuples.begin());
            WriteResult(b3);
            WriteResult(b4);
        }
        if (numBlocks % 2 != 0) {//处理剩余的奇数个块
            Block b = ReadBlock(numBlocks - 1);
            sort(b.tuples.begin(), b.tuples.end());
            WriteResult(b);
        }
        blocks = result;
        result.clear();
    }
};

/* 归并排序大致逻辑：
 * 数据集写硬盘 -> 硬盘分子区排序 -> 各区送块至缓冲区 -> 缓冲区处理得到结果送回硬盘
 * */
void mergeSort(Disk &disk, Buffer &buf) {
    // 把四个块写入磁盘
    Block block1, block2, block3, block4;
    block1.tuples = {8,9,6,4};
    block2.tuples = {2,9,5,2};
    block3.tuples = {1,1,2,6};
    block4.tuples = {0,4,2,6};
    disk.WriteBlock(block1);
    disk.WriteBlock(block2);
    disk.WriteBlock(block3);
    disk.WriteBlock(block4);
    // 对每两个块进行排序
    disk.sortBlocks();
    //每组的数量
    int numPairs = disk.getBlockSize() / GROUP_SIZE;
    //初始的两块
    for (int i = 0; i < GROUP_SIZE; ++i) {
        Block block = disk.ReadBlock(i * numPairs);
        buf.blocks.push_back(block);
    }
    map<int,int> flag,flag1;

    vector<int> s;
    for (int i = 0; i < buf.blocks.size(); ++i) {
        auto p = buf.blocks[i].tuples.front();
        buf.blocks[i].tuples.erase(buf.blocks[i].tuples.begin());
        s.push_back(p);
    }

    while (disk.getSize() < disk.getBlockSize()) {
        //取最小值
        auto min = min_element(s.begin(), s.end());
        buf.result.tuples.push_back(*min);
        auto p = min - s.begin();


        //判断tuples是否为空，为空则读取下一个块，然后size++
        s[p] = buf.blocks[p].tuples.front();

        buf.blocks[p].tuples.erase(buf.blocks[p].tuples.begin());
        if(flag[p] == 1){
                s[p] = std::numeric_limits<int>::max();
                flag[p] = 0;
                disk.addSize();
            if (disk.getSize() < disk.getBlockSize())
                continue;

        }
        else if (buf.blocks[p].tuples.empty()) {
            //如果这组都读完了，写回，标记
            if (buf.blocks[p].id % numPairs == numPairs - 1){
                flag[p] = 1;
                buf.blocks[p].tuples.push_back(s[p]);
//                s[p] = std::numeric_limits<int>::max();

            }
            else{ buf.blocks[p] = disk.ReadBlock(buf.blocks[p].id + 1);
            disk.addSize();}
        }

        //块满写结果
        if (buf.result.tuples.size() >= BLOCK_SIZE) {
            disk.WriteResult(buf.result);
            buf.result.tuples.clear();
        }
    }
    // 最后一块结果可能不满，单独写入磁盘
    if (buf.result.tuples.size() > 0) {
        disk.WriteResult(buf.result);
    }
}

void mergeSort2(Disk &disk, Buffer &buf) {
    // 把四个块写入磁盘
    Block block1, block2, block3, block4;
    block1.tuples = {8,9,6,4};
    block2.tuples = {2,9,5,2};
    block3.tuples = {1,1,2,6};
    block4.tuples = {0,4,2,6};
    disk.WriteBlock(block1);
    disk.WriteBlock(block2);
    disk.WriteBlock(block3);
    disk.WriteBlock(block4);
    // 对每两个块进行排序
    disk.sortBlocks();
    //每组的数量
    int numPairs = disk.getBlockSize() / GROUP_SIZE;
    //初始的两块
    for (int i = 0; i < GROUP_SIZE; ++i) {
        Block block = disk.ReadBlock(i * numPairs);
        buf.blocks.push_back(block);
    }
    map<int,int> flag,flag1;

    vector<int> s;
    for (int i = 0; i < buf.blocks.size(); ++i) {
        auto p = buf.blocks[i].tuples.front();
        buf.blocks[i].tuples.erase(buf.blocks[i].tuples.begin());
        s.push_back(p);
    }
    int last = numeric_limits<int>::max();
    while (disk.getSize() < disk.getBlockSize()) {
        //取最小值
        auto min = min_element(s.begin(), s.end());
        if(std::find(buf.result.tuples.begin(), buf.result.tuples.end(),*min) == buf.result.tuples.end()){
            if (last != *min){
                last = *min;
                buf.result.tuples.push_back(*min);
            }
        }

        auto p = min - s.begin();


        //判断tuples是否为空，为空则读取下一个块，然后size++
        s[p] = buf.blocks[p].tuples.front();

        buf.blocks[p].tuples.erase(buf.blocks[p].tuples.begin());
        if(flag[p] == 1){
            s[p] = std::numeric_limits<int>::max();
            flag[p] = 0;
            disk.addSize();
            if (disk.getSize() < disk.getBlockSize())
                continue;

        }
        else if (buf.blocks[p].tuples.empty()) {
            //如果这组都读完了，写回，标记
            if (buf.blocks[p].id % numPairs == numPairs - 1){
                flag[p] = 1;
                buf.blocks[p].tuples.push_back(s[p]);
//                s[p] = std::numeric_limits<int>::max();

            }
            else{ buf.blocks[p] = disk.ReadBlock(buf.blocks[p].id + 1);
                disk.addSize();}
        }

        //块满写结果
        if (buf.result.tuples.size() >= BLOCK_SIZE) {
            disk.WriteResult(buf.result);
            buf.result.tuples.clear();
        }
    }
    // 最后一块结果可能不满，单独写入磁盘
    if (buf.result.tuples.size() > 0) {
        disk.WriteResult(buf.result);
    }
}

int main() {
    Disk disk;
    Buffer buf;
    mergeSort(disk,buf);
    cout<<"不去重："<<endl;
    //输出磁盘结果信息
    int numResultBlocks = disk.getResultSize();
    for (int i = 0; i < numResultBlocks; i++) {
        Block b = disk.ReadResult(i);
        vector<int> tuples = b.tuples;
        for (int j = 0; j < tuples.size(); j++) {
            cout << tuples[j] << " ";
        }
    }
    cout<<endl<<"去重："<<endl;
    Disk d;
    Buffer b;
    mergeSort2(d,b);
    int n = d.getResultSize();
    for (int i = 0; i < n; i++) {
        Block bb = d.ReadResult(i);
        vector<int> tuples = bb.tuples;
        for (int j = 0; j < tuples.size(); j++) {
            cout << tuples[j] << " ";
        }
    }
    return 0;
}