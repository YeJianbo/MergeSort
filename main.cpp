#include<bits/stdc++.h>

using namespace std;


const int BUFFER_SIZE = 200;//��������С
const int BLOCK_SIZE = 4;//���С
const int MAX_BLOCK_NUM = 100;//������
const int MAX_DISK_SIZE = 400;//�����̿ռ�
const int GROUP_SIZE = 2;


/* ����
 * �Ӵ����ж�ȡһ���Ӽ��ϣ������õ�˳��Ŀ飬���ν���װ���ڴ棬ȡ��һ��Ԫ�������γ�һ�����Ƚϼ���
 * �����Ƚϼ����е���СԪ�ط�������飬ȡ���ڿ����һ��Ԫ�ط�����Ƚϼ��ϣ���������������������
 * */


//��
struct Block {
    vector<int> tuples;//Ԫ��
    int id;//����
    bool operator<(const Block &b) const {
        return tuples[0] < b.tuples[0];
    }
};

//������(�ڴ�)
struct Buffer {
    vector<Block> blocks;//�����
    Block result;//����飬�����飬������ʱ��������������д������Ȼ�����
//    vector<int> ptr;//��ָ��
//    int write_ptr;//дָ��
//    Buffer() : read_ptr(0), write_ptr(0) {}
};

//����
class Disk {
    vector<Block> blocks;//�洢�������ݵĿ�
//    int size;//���̴�С
    vector<Block> result;//���������Ľ��

    int size = 0;//�Ѷ�ȡ���뻺����������Ĵ�С
public:
    int WriteBlock(Block &b) {//д�鵽����
        if (blocks.size() + result.size() + 1 <= MAX_DISK_SIZE) {
            b.id = blocks.size();
            blocks.push_back(b);
            return b.id;
        }
        return -1;
    }

    int WriteResult(Block &b) {//д�鵽����
        if (blocks.size() + result.size() + 1 <= MAX_DISK_SIZE) {
            b.id = result.size();
            result.push_back(b);
            return b.id;
        }
        return -1;
    }

    Block ReadBlock(int id) {//�Ӵ��̶���
        if (id >= 0 && id < blocks.size()) {
            return blocks[id];
        }
        return Block();
    }

    Block ReadResult(int id) {//�Ӵ��̶���
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
        if (numBlocks % 2 != 0) {//����ʣ�����������
            Block b = ReadBlock(numBlocks - 1);
            sort(b.tuples.begin(), b.tuples.end());
            WriteResult(b);
        }
        blocks = result;
        result.clear();
    }
};

/* �鲢��������߼���
 * ���ݼ�дӲ�� -> Ӳ�̷��������� -> �����Ϳ��������� -> ����������õ�����ͻ�Ӳ��
 * */
void mergeSort(Disk &disk, Buffer &buf) {
    // ���ĸ���д�����
    Block block1, block2, block3, block4;
    block1.tuples = {8,9,6,4};
    block2.tuples = {2,9,5,2};
    block3.tuples = {1,1,2,6};
    block4.tuples = {0,4,2,6};
    disk.WriteBlock(block1);
    disk.WriteBlock(block2);
    disk.WriteBlock(block3);
    disk.WriteBlock(block4);
    // ��ÿ�������������
    disk.sortBlocks();
    //ÿ�������
    int numPairs = disk.getBlockSize() / GROUP_SIZE;
    //��ʼ������
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
        //ȡ��Сֵ
        auto min = min_element(s.begin(), s.end());
        buf.result.tuples.push_back(*min);
        auto p = min - s.begin();


        //�ж�tuples�Ƿ�Ϊ�գ�Ϊ�����ȡ��һ���飬Ȼ��size++
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
            //������鶼�����ˣ�д�أ����
            if (buf.blocks[p].id % numPairs == numPairs - 1){
                flag[p] = 1;
                buf.blocks[p].tuples.push_back(s[p]);
//                s[p] = std::numeric_limits<int>::max();

            }
            else{ buf.blocks[p] = disk.ReadBlock(buf.blocks[p].id + 1);
            disk.addSize();}
        }

        //����д���
        if (buf.result.tuples.size() >= BLOCK_SIZE) {
            disk.WriteResult(buf.result);
            buf.result.tuples.clear();
        }
    }
    // ���һ�������ܲ���������д�����
    if (buf.result.tuples.size() > 0) {
        disk.WriteResult(buf.result);
    }
}

void mergeSort2(Disk &disk, Buffer &buf) {
    // ���ĸ���д�����
    Block block1, block2, block3, block4;
    block1.tuples = {8,9,6,4};
    block2.tuples = {2,9,5,2};
    block3.tuples = {1,1,2,6};
    block4.tuples = {0,4,2,6};
    disk.WriteBlock(block1);
    disk.WriteBlock(block2);
    disk.WriteBlock(block3);
    disk.WriteBlock(block4);
    // ��ÿ�������������
    disk.sortBlocks();
    //ÿ�������
    int numPairs = disk.getBlockSize() / GROUP_SIZE;
    //��ʼ������
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
        //ȡ��Сֵ
        auto min = min_element(s.begin(), s.end());
        if(std::find(buf.result.tuples.begin(), buf.result.tuples.end(),*min) == buf.result.tuples.end()){
            if (last != *min){
                last = *min;
                buf.result.tuples.push_back(*min);
            }
        }

        auto p = min - s.begin();


        //�ж�tuples�Ƿ�Ϊ�գ�Ϊ�����ȡ��һ���飬Ȼ��size++
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
            //������鶼�����ˣ�д�أ����
            if (buf.blocks[p].id % numPairs == numPairs - 1){
                flag[p] = 1;
                buf.blocks[p].tuples.push_back(s[p]);
//                s[p] = std::numeric_limits<int>::max();

            }
            else{ buf.blocks[p] = disk.ReadBlock(buf.blocks[p].id + 1);
                disk.addSize();}
        }

        //����д���
        if (buf.result.tuples.size() >= BLOCK_SIZE) {
            disk.WriteResult(buf.result);
            buf.result.tuples.clear();
        }
    }
    // ���һ�������ܲ���������д�����
    if (buf.result.tuples.size() > 0) {
        disk.WriteResult(buf.result);
    }
}

int main() {
    Disk disk;
    Buffer buf;
    mergeSort(disk,buf);
    cout<<"��ȥ�أ�"<<endl;
    //������̽����Ϣ
    int numResultBlocks = disk.getResultSize();
    for (int i = 0; i < numResultBlocks; i++) {
        Block b = disk.ReadResult(i);
        vector<int> tuples = b.tuples;
        for (int j = 0; j < tuples.size(); j++) {
            cout << tuples[j] << " ";
        }
    }
    cout<<endl<<"ȥ�أ�"<<endl;
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