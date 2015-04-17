package org.petuum.ps.common.util;

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntObjectMap;
import org.petuum.ps.config.HostInfo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class Utils {

    public static void getHostInfos(String server_file,
                                    Map<Integer, HostInfo> host_map) {
        try (BufferedReader br = new BufferedReader(new FileReader(server_file))) {
            for (String line; (line = br.readLine()) != null; ) {
                int pos = line.indexOf(" ");
                String idstr = line.substring(0, pos);

                int pos_ip = line.indexOf(" ", pos + 1);
                String ip = line.substring(pos + 1, pos_ip - pos + 1);

                String port = line.substring(pos_ip + 1);

                int id = Integer.parseInt(idstr);

                host_map.put(id, new HostInfo(id, ip, port));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void GetServerIDsFromHostMap(ArrayList<Integer> server_ids,
                                               TIntObjectMap<HostInfo> host_map) {

        int num_servers = host_map.size() - 1;
        server_ids.ensureCapacity(num_servers);

        int i = 0;
        for (TIntObjectIterator<HostInfo> host_entry = host_map.iterator(); host_entry
                .hasNext(); ) {
            host_entry.advance();
            if (host_entry.key() == 0) {
                continue;
            }
            server_ids.add(i, host_entry.key());
            i++;
        }
    }

}
