// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.service;

import com.starrocks.common.Config;
import com.starrocks.common.util.NetUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertThrows;


public class FrontendOptionsTest {

    @Mocked
    Inet4Address addr;

    private boolean useFqdn = true;
    private boolean useFqdnFile = true;

    @Test
    public void cidrTest() {

        List<String> priorityCidrs = FrontendOptions.PRIORITY_CIDRS;
        priorityCidrs.add("192.168.5.136/32");
        priorityCidrs.add("2001:db8::/32");

        FrontendOptions frontendOptions = new FrontendOptions();
        boolean inPriorNetwork = frontendOptions.isInPriorNetwork("127.0.0.1");
        Assertions.assertEquals(false, inPriorNetwork);

        inPriorNetwork = frontendOptions.isInPriorNetwork("192.168.5.136");
        Assertions.assertEquals(true, inPriorNetwork);
        
        inPriorNetwork = frontendOptions.isInPriorNetwork("2001:db8::1");
        Assertions.assertTrue(inPriorNetwork);
    }

    @Test
    public void cidrTest2() {
        List<String> priorityCidrs = FrontendOptions.PRIORITY_CIDRS;
        priorityCidrs.add("2408:4001:258::/48");

        FrontendOptions frontendOptions = new FrontendOptions();
        boolean inPriorNetwork = frontendOptions.isInPriorNetwork("2408:4001:258:3780:f3f4:5acd:d53d:fa23");
        Assertions.assertEquals(true, inPriorNetwork);
    }

    private void mockNet() {
        new MockUp<Inet4Address>() {
            @Mock
            public InetAddress getLocalHost() throws UnknownHostException {
                return addr;
            }
            @Mock
            public String getHostAddress() {
                return "127.0.0.10";
            }
            @Mock
            public String getCanonicalHostName() {
                return "sandbox";
            }
        };
        new MockUp<NetUtils>() {
            @Mock
            public List<InetAddress> getHosts() {
                List<InetAddress> hosts = new ArrayList<>();
                hosts.add(addr);
                return hosts;
            }
        };

        new MockUp<FrontendOptions>() {
            @Mock
            public void initAddrUseFqdn(List<InetAddress> hosts) throws UnknownHostException {
                useFqdn = true;
                useFqdnFile = true;
            }
            @Mock
            public void initAddrUseIp(List<InetAddress> hosts) {
                useFqdn = false;
                useFqdnFile = false;
            }
        };
    }

    @Test
    public void enableFQDNTest() throws UnknownHostException,
                                        NoSuchFieldException,
                                        SecurityException,
                                        IllegalArgumentException,
                                        IllegalAccessException {
        mockNet();
        Field field = FrontendOptions.class.getDeclaredField("localAddr");
        field.setAccessible(true);
        field.set(null, addr);
        Field field1 = FrontendOptions.class.getDeclaredField("useFqdn");
        field1.setAccessible(true);

        field1.set(null, true);
        Assertions.assertTrue(FrontendOptions.getLocalHostAddress().equals("sandbox"));
        field1.set(null, false);
        Assertions.assertTrue(FrontendOptions.getLocalHostAddress().equals("127.0.0.10"));
    }

    @Test
    public void testChooseHostType() throws UnknownHostException {
        mockNet();
        useFqdn = true;
        FrontendOptions.init("ip");
        Assertions.assertTrue(!useFqdn);
        useFqdn = false;
        FrontendOptions.init("fqdn");
        Assertions.assertTrue(useFqdn);
        useFqdn = false;
        FrontendOptions.init(null);
        Assertions.assertTrue(!useFqdn);
    }

    private void testInitAddrUseFqdnCommonMock() {
        new MockUp<System>() {
            @Mock
            public void exit(int status) throws IllegalAccessException {
                throw new IllegalAccessException();
            }
        };
        new MockUp<NetUtils>() {
            @Mock
            public List<InetAddress> getHosts() {
                List<InetAddress> hosts = new ArrayList<>();
                hosts.add(addr);
                return hosts;
            }
        };
    }

    @Test
    public void testGetStartWithFQDNThrowUnknownHostException() {
        assertThrows(IllegalAccessException.class, () -> {
            String oldVal = Config.priority_networks;
            Config.priority_networks = "";
            testInitAddrUseFqdnCommonMock();
            List<InetAddress> hosts = NetUtils.getHosts();
            new MockUp<InetAddress>() {
                @Mock
                public InetAddress getLocalHost() throws UnknownHostException {
                    throw new UnknownHostException();
                }
            };
            FrontendOptions.initAddrUseFqdn(hosts);
            Config.priority_networks = oldVal;
        });
    }

    @Test
    public void testGetStartWithFQDNGetNullCanonicalHostName() {
        assertThrows(IllegalAccessException.class, () -> {
            testInitAddrUseFqdnCommonMock();
            List<InetAddress> hosts = NetUtils.getHosts();
            new MockUp<InetAddress>() {
                @Mock
                public InetAddress getLocalHost() throws UnknownHostException {
                    return addr;
                }

                @Mock
                public String getHostAddress() {
                    return "127.0.0.10";
                }

                @Mock
                public String getCanonicalHostName() {
                    return null;
                }
            };
            FrontendOptions.initAddrUseFqdn(hosts);
        });
    }

    @Test
    public void testGetStartWithFQDNGetNameThrowUnknownHostException() {
        assertThrows(IllegalAccessException.class, () -> {
            testInitAddrUseFqdnCommonMock();
            List<InetAddress> hosts = NetUtils.getHosts();
            new MockUp<InetAddress>() {
                @Mock
                public InetAddress getLocalHost() throws UnknownHostException {
                    return addr;
                }

                @Mock
                public String getHostAddress() {
                    return "127.0.0.10";
                }

                @Mock
                public String getCanonicalHostName() {
                    return "sandbox";
                }

                @Mock
                public InetAddress getByName(String host) throws UnknownHostException {
                    throw new UnknownHostException();
                }
            };
            FrontendOptions.initAddrUseFqdn(hosts);
        });
    }

    @Test
    public void testGetStartWithFQDNGetNameGetNull() {
        assertThrows(IllegalAccessException.class, () -> {
            testInitAddrUseFqdnCommonMock();
            List<InetAddress> hosts = NetUtils.getHosts();
            new MockUp<InetAddress>() {
                @Mock
                public InetAddress getLocalHost() throws UnknownHostException {
                    return addr;
                }

                @Mock
                public String getHostAddress() {
                    return "127.0.0.10";
                }

                @Mock
                public String getCanonicalHostName() {
                    return "sandbox";
                }

                @Mock
                public InetAddress getByName(String host) throws UnknownHostException {
                    return null;
                }
            };
            FrontendOptions.initAddrUseFqdn(hosts);
        });
    }

    @Test
    public void testGetStartWithFQDN() {
        testInitAddrUseFqdnCommonMock();
        List<InetAddress> hosts = NetUtils.getHosts();
        new MockUp<InetAddress>() {
            @Mock
            public InetAddress getLocalHost() throws UnknownHostException {
                return addr;
            }
            @Mock
            public String getHostAddress() {
                return "127.0.0.10";
            }
            @Mock
            public String getCanonicalHostName() {
                return "sandbox";
            }
            @Mock
            public InetAddress getByName(String host) throws UnknownHostException {
                return addr;
            }
        };
        FrontendOptions.initAddrUseFqdn(hosts);
    }

    @Test
    public void testGetStartWithFQDNNotFindAddr() {
        assertThrows(IllegalAccessException.class, () -> {
            new MockUp<System>() {
                @Mock
                public void exit(int status) throws IllegalAccessException {
                    throw new IllegalAccessException();
                }
            };
            new MockUp<NetUtils>() {
                @Mock
                public List<InetAddress> getHosts() {
                    List<InetAddress> hosts = new ArrayList<>();
                    return hosts;
                }
            };
            List<InetAddress> hosts = NetUtils.getHosts();
            new MockUp<InetAddress>() {
                @Mock
                public InetAddress getLocalHost() throws UnknownHostException {
                    return addr;
                }

                @Mock
                public String getHostAddress() {
                    return "127.0.0.10";
                }

                @Mock
                public String getCanonicalHostName() {
                    return "sandbox";
                }

                @Mock
                public InetAddress getByName(String host) throws UnknownHostException {
                    return addr;
                }
            };
            FrontendOptions.initAddrUseFqdn(hosts);
        });
    }

    private void mkdir(boolean hasFqdn, String metaPath) {
        File dir = new File(metaPath);
        if (!dir.exists()) {
            dir.mkdir();
        } else {
            deleteDir(dir);
        }
        File dirImage = new File(metaPath + "image/");
        dirImage.mkdir();
        File version = new File(metaPath + "image/ROLE");
        try {
            version.createNewFile();
            String line1 = "#Mon Feb 02 13:59:54 CST 2022\n";
            String line2 = "hostType=FQDN\n";
            String line3 = "name=172.26.92.139_3114_1652846479257\n";
            String line4 = "role=FOLLOWER\n";
            FileWriter fw = new FileWriter(version);
            fw.write(line3);
            fw.write(line4);
            fw.write(line1);
            if (hasFqdn) {
                fw.write(line2);
            }
            fw.flush();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void deleteDir(File dir) {

        if (!dir.exists()) {
            return;
        }
        if (dir.isFile()) {
            dir.delete();
        } else {
            for (File file : dir.listFiles()) {
                deleteDir(file);
            }
        }
        dir.delete();
    }

    @Test
    public void testChooseHostTypeByFile() throws UnknownHostException {
        mockNet();
        Config.meta_dir = "feOpTestDir1";
        String metaPath = Config.meta_dir + "/";
        // fqdn
        mkdir(true, metaPath);
        useFqdnFile = false;
        FrontendOptions.init(null);
        Assertions.assertTrue(useFqdnFile);
        File dir = new File(metaPath);
        deleteDir(dir);
        // ip
        mkdir(false, metaPath);
        useFqdnFile = true;
        FrontendOptions.init(null);
        Assertions.assertTrue(!useFqdnFile);
        dir = new File(metaPath);
        deleteDir(dir);
    }

    @Test
    public void testSaveStartType() throws FileNotFoundException, IOException {
        Config.meta_dir = "feOpTestDir2";
        String metaPath = Config.meta_dir + "/";
        // fqdn
        File dir = new File(metaPath);
        deleteDir(dir);
        mkdir(false, metaPath);
        FrontendOptions.saveStartType();
        String roleFilePath = Config.meta_dir + "/image/ROLE";
        File roleFile = new File(roleFilePath);

        Properties prop = new Properties();
        String hostType;
        try (FileInputStream in = new FileInputStream(roleFile)) {
            prop.load(in);
        }
        hostType = prop.getProperty("hostType", null);
        Assertions.assertTrue((hostType.equals("IP") || hostType.equals("FQDN")));
        dir = new File(metaPath);
        deleteDir(dir);
    }
}
