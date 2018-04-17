#include <bits/stdc++.h>
#include "mpi.h"
// #include <assert.h>

using namespace std;
#define pb push_back
#define mp make_pair
typedef pair<float,char*> fs;

int cols;
int maxr=-1;
vector<vector<fs> > matrix;
vector<int> numr;

void print_matrix()
{
	for (int i = 0; i < maxr; ++i)
	{
		for (int j = 0; j < numr[i]; ++j)
		{
			fs temp = matrix[j][i];
			// if(temp.second=="623")
			// {break;}
			// const char *st = temp.second.c_str();
			cout << "(" << temp.first << "," << temp.second << ")";
		}
		cout << endl;
	}
}

void take_input(string base)
{
	// #pragma omp parallel for
	for(int j=0;j<cols;++j)
	{


		ifstream file((base.c_str() + to_string(static_cast<long long>(j + 1))).c_str(), ios::in | ios::binary);
		if (file.is_open())
		{
		int b1 = file.get();
		int b2 = file.get();
		int b3 = file.get();
		int b4 = file.get();

		int n = (int)((unsigned char)(b4) << 24 |
					  (unsigned char)(b3) << 16 |
					  (unsigned char)(b2) << 8 |
					  (unsigned char)(b1));
		// cout<<"n="<<n<<endl;

			while (!file.eof())
			{
				float key;


				file.read(reinterpret_cast<char *>(&key),sizeof(float)) ;
				char *s = (char *)malloc((n+1) * sizeof(char));
				file.read(s, n );
				s[n]='\0';

				if(file.eof())
				{break;}

				// string str(s,n);
				// assert (str.length()==n);
				int selected=j,l=matrix[j].size();

				// for(int i=0;i<j;++i)
				// {
				// 	if(matrix[i].size()<=l)
				// 	{selected=i;break;}
				// }

				matrix[selected].pb(mp(key,s));
				
				// cout<<key<<" **"<<str<<"**"<<endl;
				
			}
			// --r;
			// matrix[j].pop_back();



			file.close();
		}
		else
			cout << "Error in  opening file";
	}

	maxr=-1;
	for (int j = 0; j < cols; ++j)
	{
		int r=matrix[j].size();
		if (maxr < r)
			maxr = r;
	}
}

void fill_holes()
{
	for(int j=0;j<cols;++j)
	{
		char *s = (char *)malloc((5) * sizeof(char));
		s[0]='6';
		s[1] = '2';
		s[2] = '3';
		s[3]='\0';
		fs temp = mp(FLT_MAX, s);
		int start = matrix[j].size() ;
		for (int i = start; i < maxr; ++i)
		{
			matrix[j].pb(temp);
		}
	}
}
// void move_holes()
// {
// 	for()
// 	{}
// }

int check_sorted()
{

}
void dosorting()
{
	//sort rows
	int min_idx,sw=1,f=1;
	
while(f==1)
{	
	// cout<<"sorting"<<endl;

	sw = 0;
	
	for (int i = 0; i < maxr; ++i)
	{	
		int size=numr[i];
		// for (int j = 0; j < size - 1; ++j)
		// {
		// 	// if (all[i].aa[X].key == 0)
		// 	// 	continue;
		// 	min_idx = j;
		// 	float min_v = matrix[j][i].first;
		// 	for (int k = j + 1; k < size; ++k)
		// 	{
		// 		// if (all[j].aa[X].key == 0)
		// 		// 	continue;
		// 		if (matrix[k][i].first < min_v)
		// 		{
		// 			min_idx = k;
		// 			min_v = matrix[k][i].first;
		// 			sw++;
		// 		}
		// 	}
		// 	// temp = all[min_idx].aa[X];
		// 	// all[min_idx].aa[X] = all[i].aa[X];
		// 	// all[i].aa[X] = temp;
		// 	if(min_idx>j)
		// 	swap(matrix[j][i], matrix[min_idx][i]);
		// }

		vector<fs> temp;
		for(int j=0;j<size;++j)
		{temp.pb(matrix[j][i]);}
		sort(temp.begin(),temp.end());
		for (int j = 0; j < size; ++j)
		{
			matrix[j][i]=temp[j];
		}
	}
	// print_matrix();

	//sort columns
	for (int j=0 ; j < cols; ++j)
	{
		sort(matrix[j].begin(), matrix[j].end());
	}

	for (int j = 0; j < cols; ++j)
	{
		int size=matrix[j].size();
		for(int i=1;i<size;++i)
		{
			if(matrix[j][i-1].first>matrix[j][i].first)
			{f=1;goto l;}
		}
	}
	for (int i = 0; i < maxr; ++i)
	{
		int size=numr[i];
		for (int j = 1; j < size; ++j)
		{
			if (matrix[j-1][i].first > matrix[j][i].first)
			{
				f = 1;
				goto l;
			}
		}
	}
	// print_matrix();
	f=0;
	l:;
	// cout<<"f="<<f<<endl;
}
	
}


void give_output(string base)
{

	ofstream file((base + to_string(static_cast<long long> (0))).c_str(), ios::binary | ios::out);
	if(!file.is_open())
	{cout<<"Output file not opened"<<endl;return;}

	for(int i=0;i<maxr;++i)
	{

		for(int j=0;j<numr[i];++j)
		{
			fs temp=matrix[j][i];

			if(strcmp(temp.second,"623")==0)
			{continue;}
			
			// const char *st = temp.second.c_str();	
			int l = strlen(temp.second);
			// assert(strlen(st)==l);
			
			float key = temp.first;

			// cout<<"("<<key<<","<<st<<")";
			file.write(reinterpret_cast<char *>(&key), 4);
			file.write(temp.second, l);
		}
		// cout<<endl;
	}
	file.close();
}

int main(int argc,char* argv[])
{

MPI_Init(NULL, NULL);
// start MPI 	
int numProc, myRank;
MPI_Comm_size(MPI_COMM_WORLD, &numProc);// Group size 	
MPI_Comm_rank(MPI_COMM_WORLD, &myRank); // get my rank 	


if (myRank == 0)
{
maxr=-1;

cols=atoi(argv[1]);
char* BaseFile=argv[2];
string basefile(BaseFile);

matrix.resize(cols);

take_input(basefile);

fill_holes();

numr.resize(maxr,0);
for(int i=0;i<cols;++i)
{
	numr[matrix[i].size()-1]+=1;
}

for(int i=maxr-2;i>=0;--i)
{
	numr[i]+=numr[i+1];
}


//move holes to the right
// move_holes();

// print_matrix();

dosorting();
// cout<<"sorting done"<<endl;

give_output(basefile);
}

MPI_Finalize();
return 0;
}