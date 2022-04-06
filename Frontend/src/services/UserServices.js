import axios from "axios";
import { USERURL } from "../constants/Constants";

export const getData = limit => axios.post(USERURL+limit)