package zy_juc.v20220531.capter3CompleteFuture;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * ClassName LamdaTest
 *
 * @Auther: 赵繁旗
 * @Date: 2022/6/14 09:44
 * @Description:
 */
public class LamdaTest {
    public static void main(String[] args) {
        Student student = new Student();
        Student studentChain = new Student();
        student.setStudentId(1);
        student.setStudentName("zhangsan");
        student.setMajor("English");
        System.out.println(student);

        studentChain.setMajor("compute").setStudentName("zhaoliu").setStudentId(10);
        System.out.println(studentChain);
    }
}

@AllArgsConstructor
@NoArgsConstructor
@Data
@Accessors(chain=true)
class Student{
    private int studentId;
    private String studentName;
    private String major;
}
