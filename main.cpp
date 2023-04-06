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

    int size;//已读取进入缓冲区处理过的大小
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

    void ReadResult(int id, Block &b) {//从磁盘读块
        if (id >= 0 && id < result.size()) {
            b = result[id];
        }
    }

    int getBlockSize() {
        return blocks.size();
    }

    int getSize() {
        return size;
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


void mergeSort(Disk &disk, Buffer &buf) {
    // 把四个块写入磁盘
    Block block1, block2, block3, block4;
    block1.tuples = {10, 9, 8, 7};
    block2.tuples = {6, 5, 4, 3};
    block3.tuples = {2, 1, 0, -1};
    block4.tuples = {-2, -3, -4, -5};
    disk.WriteBlock(block1);
    disk.WriteBlock(block2);
    disk.WriteBlock(block3);
    disk.WriteBlock(block4);

    // 对每两个块进行排序
    disk.sortBlocks();

    // 归并排序
//    while (disk.getBlockSize() > 1) {
//        int numPairs = disk.getBlockSize() / 2;
    //初始的两块

    //注意：此处保留！需要编写代码将数据从磁盘读入缓冲区

    vector<int> s;
    for (int i = 0; i < buf.blocks.size(); ++i) {
        auto p = buf.blocks[i].tuples.front();
        buf.blocks[i].tuples.erase(buf.blocks[i].tuples.begin());
        s.push_back(p);
    }

    while (disk.getSize() >= disk.getBlockSize()) {
        auto min = min_element(s.begin(),s.end());
        buf.result.tuples.push_back(*min);
        auto p = min - s.begin();
        //注意，此处应判断tuples是否为空，为空则读取下一个块，然后size++，此处暂时省略
        s[p] = buf.blocks[p].tuples.front();
        buf.blocks[p].tuples.erase(buf.blocks[p].tuples.begin());
    }

    for (int i = 0; i < numPairs; i++) {
        // 从磁盘中读入两个块


        // 初始化归并结果块
        Block mergedBlock;
        mergedBlock.tuples.reserve(b1.tuples.size() + b2.tuples.size());

        // 归并排序
        while (buf.blocks.size() > 0) {
            // 找到当前最小值的块
            int minBlock = -1;
            for (int j = 0; j < buf.blocks.size(); j++) {
                if (buf.ptr[j] < buf.blocks[j].tuples.size()) {
                    if (minBlock == -1 ||
                        buf.blocks[j].tuples[buf.ptr[j]] < buf.blocks[minBlock].tuples[buf.ptr[minBlock]]) {
                        minBlock = j;
                    }
                }
            }

            // 找到了最小值块
            if (minBlock != -1) {
                // 将最小值加入归并结果块
                mergedBlock.tuples.push_back(buf.blocks[minBlock].tuples[buf.ptr[minBlock]]);
                buf.ptr[minBlock]++;

                // 当缓冲区满时，将归并结果块写入磁盘
                if (mergedBlock.tuples.size() == BUFFER_SIZE) {
                    disk.WriteResult(mergedBlock);
                    mergedBlock.tuples.clear();
                }

                // 如果当前块已经处理完了，从缓冲区中删除
                if (buf.ptr[minBlock] == buf.blocks[minBlock].tuples.size()) {
                    buf.blocks.erase(buf.blocks.begin() + minBlock);
                    buf.ptr.erase(buf.ptr.begin() + minBlock);
                }
            }
        }

        // 缓冲区为空时，将剩余部分的归并结果块写入磁盘
        if (mergedBlock.tuples.size() > 0) {

        }
    }
//    }
}


int main() {
    Disk disk;
    //为块手动赋值，并保存到硬盘中
//    vector<Block> blocks;
    Block block = {{1, 9, 8, 9}, 0};
    Block block2 = {{0, 6, 0, 4}, 1};
    Block block3 = {{2, 9, 5, 2}, 2};
    Block block4 = {{1, 1, 2, 6}, 3};
//    blocks.push_back(block);
    disk.WriteBlock(block);
    disk.WriteBlock(block2);
    disk.WriteBlock(block3);
    disk.WriteBlock(block4);
    //将硬盘数据分得的两块排序
    disk.sortBlocks();
    //取硬盘数据，送缓冲区
    Buffer buf;
    buf.blocks.push_back(disk.ReadBlock(0));
    buf.blocks.push_back(disk.ReadBlock(disk.getBlockSize() / GROUP_SIZE));
    //取缓冲区的第一个
    vector<int> s;
    for (int i = 0; i < buf.blocks.size(); i++) {
        s.push_back(buf.blocks[i].tuples[buf.ptr[i]]);
        buf.ptr[i]++;
    }
    Block b;
    //循环取最小值放入缓冲区，结果缓冲块满存入磁盘，输入指针到底则从硬盘读取下一块，直到所有排序结束
    while (1) {
        auto a = std::min_element(s.begin(), s.end());
        b.tuples.push_back(*a);
        auto pos = a - s.begin();
        s[pos] = buf.blocks[pos].tuples[++buf.ptr[pos]];
    }

//    disk.WriteResult()
//    s.insert()
}