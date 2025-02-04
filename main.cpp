#include <iostream>
#include <fstream>
#include <string>

// Function to check if a file exists
static bool FileExists(const std::string &Filename) 
{
    std::ifstream FileHandler(Filename);
    return FileHandler.good();
}

#define MYFILE		("./system_timestamp.txt")

int main() 
{
    const std::string filename = MYFILE;

    // Check if the file exists
    if (FileExists(filename)) 
    {
        std::cout << "The file '" << filename << "' exists.\n";
    } 
    else 
    {
        std::cout << "The file '" << filename << "' does not exist.\n";
    }

    return 0;
}

