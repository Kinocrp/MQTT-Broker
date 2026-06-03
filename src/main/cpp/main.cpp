#include <iostream>
#include <iomanip>
#include <vector>
#include <unordered_map>
#include <string>
#include <thread>
#include <mutex>
#include <winsock2.h>
#include <ws2tcpip.h>

#pragma comment(lib, "Ws2_32.lib")

std::unordered_map<std::string, std::vector<SOCKET>> topic_subscribers;
std::mutex broker_mutex;

/*
void printHex(const char* buffer, int length) {
    for (int i = 0; i < length; ++i) {
        std::cout << std::hex << std::setw(2) << std::setfill('0') << std::uppercase 
                  << (static_cast<unsigned int>(buffer[i]) & 0xFF) << " ";
    }
    std::cout << std::dec << std::endl;
}
*/

void handleClient(SOCKET clientSocket) {
    char buffer[4096];

    while (true) {
        int bytesReceived = recv(clientSocket, buffer, sizeof(buffer), 0);

        if (bytesReceived <= 0) {
            std::cout << "\n[DISCONNECTED] Socket ID: " << clientSocket << " connection lost." << std::endl;

            {
                std::lock_guard<std::mutex> lock(broker_mutex);

                for (auto it = topic_subscribers.begin(); it != topic_subscribers.end(); ) {
                    std::vector<SOCKET>& subscribers = it->second;

                    subscribers.erase(
                        std::remove(subscribers.begin(), subscribers.end(), clientSocket),
                        subscribers.end()
                    );

                    if (subscribers.empty()) {
                        std::cout << "[CLEAN] Topic '" << it->first << "' is empty. Removing from map." << std::endl;
                        it = topic_subscribers.erase(it);
                    } else {
                        ++it;
                    }
                }
            }
            std::cout << "[CLEAN] Socket " << clientSocket << " fully removed." << std::endl;

            closesocket(clientSocket);
            break;
        }

        int offset = 0;
        while (offset < bytesReceived) {

            unsigned char packetType = buffer[offset] & 0xF0;

            int multiplier = 1;
            int remainingLength = 0;
            int headerLength = 1;
            unsigned char encodedByte;

            do {
                if (offset + headerLength >= bytesReceived) break;

                encodedByte = buffer[offset + headerLength];
                remainingLength += (encodedByte & 127) * multiplier;
                multiplier *= 128;
                headerLength++;
            } while ((encodedByte & 128) != 0);

            int totalPacketSize = headerLength + remainingLength;
            char* currentPacket = buffer + offset;

            switch (packetType) {
                case 0x10: {
                    std::cout << "\n[CONNECT] Received." << std::endl;

                    unsigned char keepAliveMsb = currentPacket[headerLength + 8];
                    unsigned char keepAliveLsb = currentPacket[headerLength + 9];
                    int keepAliveSeconds = (keepAliveMsb << 8) | keepAliveLsb;

                    std::cout << "          -> Client Keep-Alive: " << keepAliveSeconds << " seconds." << std::endl;

                    if (keepAliveSeconds > 0) {
                        int timeoutMs = keepAliveSeconds * 1500;
                        DWORD timeout = timeoutMs;
                        setsockopt(clientSocket, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof(timeout));
                        std::cout << "          -> Socket will auto-kill if silent for " << timeoutMs << " ms." << std::endl;
                    }

                    char connack[] = { 0x20, 0x02, 0x00, 0x00 };
                    send(clientSocket, connack, sizeof(connack), 0);
                    break;
                }

                case 0x80: {
                    unsigned char packetIdMsb = currentPacket[headerLength];
                    unsigned char packetIdLsb = currentPacket[headerLength + 1];

                    std::cout << "\n[SUBSCRIBE] Packet ID: " << (int)((packetIdMsb << 8) | packetIdLsb) << std::endl;
                    std::vector<char> subackPayload;
                    int payloadOffset = headerLength + 2;

                    while (payloadOffset < totalPacketSize) {
                        int topicLen = (static_cast<unsigned char>(currentPacket[payloadOffset]) << 8) | static_cast<unsigned char>(currentPacket[payloadOffset + 1]);
                        payloadOffset += 2;

                        std::string topic(currentPacket + payloadOffset, topicLen);
                        payloadOffset += topicLen;

                        unsigned char qos = currentPacket[payloadOffset];
                        payloadOffset += 1;

                        std::cout << "  -> Extracted Topic: '" << topic << "'" << std::endl;

                        {
                            std::lock_guard<std::mutex> lock(broker_mutex);
                            std::vector<SOCKET>& subs = topic_subscribers[topic];
                            if (std::find(subs.begin(), subs.end(), clientSocket) == subs.end()) {
                                subs.push_back(clientSocket);
                                std::cout << "          => Added new subscriber." << std::endl;
                            } else {
                                std::cout << "          => Duplicate subscription ignored." << std::endl;
                            }
                        }

                        subackPayload.push_back(0x00);
                    }

                    int subackRemainingLen = 2 + subackPayload.size();
                    std::vector<char> subackPacket;
                    subackPacket.push_back(static_cast<char>(0x90));
                    subackPacket.push_back(static_cast<char>(subackRemainingLen));
                    subackPacket.push_back(packetIdMsb);
                    subackPacket.push_back(packetIdLsb);
                    for (char resQos : subackPayload) {
                        subackPacket.push_back(resQos);
                    }

                    send(clientSocket, subackPacket.data(), subackPacket.size(), 0);
                    break;
                }

                case 0x30: {
                    int topicLen = (static_cast<unsigned char>(currentPacket[headerLength]) << 8) | static_cast<unsigned char>(currentPacket[headerLength + 1]);
                    std::string topic(currentPacket + headerLength + 2, topicLen);
                    std::cout << "\n[PUBLISH] Topic: '" << topic << "'" << std::endl;

                    std::lock_guard<std::mutex> lock(broker_mutex);
                    if (topic_subscribers.find(topic) != topic_subscribers.end()) {
                        int targetCount = 0;
                        for (SOCKET targetSocket : topic_subscribers[topic]) {
                            if (targetSocket != clientSocket) {
                                send(targetSocket, currentPacket, totalPacketSize, 0);
                                targetCount++;
                            }
                        }
                        std::cout << "          => Forwarded to " << targetCount << " subscriber(s)!" << std::endl;
                    } else {
                        std::cout << "          => No subscribers. Dropped." << std::endl;
                    }
                    break;
                }

                case 0xC0: {
                    char pingresp[] = { static_cast<char>(0xD0), 0x00 };
                    send(clientSocket, pingresp, sizeof(pingresp), 0);
                    break;
                }

                default:
                    break;
            }
            offset += totalPacketSize;
        }
    }
}

int main() {
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2, 2), &wsaData);

    SOCKET serverSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    sockaddr_in serverAddr;
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port = htons(1883);

    bind(serverSocket, (SOCKADDR*)&serverAddr, sizeof(serverAddr));
    listen(serverSocket, SOMAXCONN);

    std::cout << "========================================" << std::endl;
    std::cout << "  C++ MQTT Broker Started               " << std::endl;
    std::cout << "  Listening on TCP Port 1883...         " << std::endl;
    std::cout << "========================================" << std::endl;

    while (true) {
        sockaddr_in clientAddr;
        int clientAddrSize = sizeof(clientAddr);
        SOCKET clientSocket = accept(serverSocket, (SOCKADDR*)&clientAddr, &clientAddrSize);

        if (clientSocket != INVALID_SOCKET) {
            std::cout << "\n[NEW CONNECTION] Client connected! Socket ID: " << clientSocket << std::endl;
            std::thread(handleClient, clientSocket).detach();
        }
    }

    closesocket(serverSocket);
    WSACleanup();
    return 0;
}
